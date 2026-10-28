// Web UI for watching the hotspot live and controlling its links

use crate::allstar;
use crate::config::{Action, Config};
use crate::hosts::{self, Directory};
use crate::metrics::Metrics;
use crate::reflector::Command;
use crate::state::{self, Shared};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::interval;

const PAGE: &str = include_str!("page.html");

// Changes to the status are sent in batches, so a busy link doesn't flood the page
const SNAPSHOT_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone)]
struct App {
    config: Config,
    directory: Directory,
    shared: Shared,
    commands: mpsc::Sender<Command>,
    allstar: Option<allstar::Handle>,
    metrics: Arc<Metrics>,
}

// Checkboxes are only sent when ticked
#[derive(Deserialize)]
struct AllstarLinkForm {
    node: String,
    monitor: Option<String>,
    permanent: Option<String>,
}

#[derive(Deserialize)]
struct AllstarUnlinkForm {
    node: String,
}

#[derive(Deserialize)]
struct LinkForm {
    reflector: String,
    module: String,
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

async fn index(State(app): State<App>) -> Html<String> {
    let options: String = hosts::names(&app.directory).iter().map(|name| format!("<option value=\"{}\">", escape(name))).collect();
    let station = &app.config.station;
    Html(
        PAGE.replace("{{callsign}}", &escape(&station.callsign))
            .replace("{{latitude}}", &station.latitude.to_string())
            .replace("{{longitude}}", &station.longitude.to_string())
            .replace("{{reflector_options}}", &options),
    )
}

async fn status(State(app): State<App>) -> Json<state::Snapshot> {
    let mut snapshot = state::read(&app.shared);
    if let Some(current) = snapshot.current.as_mut() {
        current.seconds = state::unix_now().saturating_sub(current.started_at);
    }
    Json(snapshot)
}

// The status as it is now, then each change to it, along with the events as they happen
async fn stream(app: App, mut socket: WebSocket) {
    let mut snapshots = app.shared.subscribe();
    let mut events = app.shared.subscribe_events();
    let mut batch = interval(SNAPSHOT_INTERVAL);
    let mut changed = true;

    // Where voice has been going lately, which events then keep up to date
    let oldest = state::unix_now().saturating_sub(state::TRAFFIC_WINDOW_SECONDS);
    let history: Vec<state::Transmission> = state::read(&app.shared).history.into_iter().filter(|transmission| transmission.started_at + transmission.seconds >= oldest).collect();
    if socket.send(Message::Text(serde_json::json!({ "type": "history", "transmissions": history }).to_string().into())).await.is_err() {
        return;
    }
    loop {
        let message = tokio::select! {
            result = snapshots.changed() => {
                if result.is_err() {
                    return;
                }
                changed = true;
                continue;
            }
            _ = batch.tick() => {
                if !std::mem::take(&mut changed) {
                    continue;
                }
                serde_json::json!({ "type": "snapshot", "snapshot": state::read(&app.shared) })
            }
            event = events.recv() => match event {
                Ok(event) => serde_json::json!(event),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return,
            },
            received = socket.recv() => match received {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => continue,
            },
        };
        if socket.send(Message::Text(message.to_string().into())).await.is_err() {
            return;
        }
    }
}

async fn live(State(app): State<App>, upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(move |socket| stream(app, socket))
}

// What Prometheus scrapes
async fn metrics(State(app): State<App>) -> impl IntoResponse {
    app.metrics.update(&state::read(&app.shared));
    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")], app.metrics.encode())
}

async fn link(State(app): State<App>, Form(form): Form<LinkForm>) -> Response {
    let module = form.module.trim().to_ascii_uppercase();
    let known = hosts::find(&app.directory, form.reflector.trim()).is_some();
    match (known, module.chars().next().filter(|letter| module.len() == 1 && letter.is_ascii_uppercase())) {
        (true, Some(module)) => {
            let _ = app.commands.send(Command::Link { name: form.reflector.trim().to_string(), module }).await;
            Redirect::to("/").into_response()
        }
        _ => (StatusCode::BAD_REQUEST, "Unknown reflector, or the module isn't a single letter").into_response(),
    }
}

async fn unlink(State(app): State<App>) -> Redirect {
    let _ = app.commands.send(Command::Unlink).await;
    Redirect::to("/")
}

async fn allstar_action(app: &App, action: Action) -> Response {
    let Some(handle) = &app.allstar else { return (StatusCode::NOT_FOUND, "AllStar isn't configured").into_response() };
    if let Err(error) = allstar::step(&action) {
        return (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response();
    }
    match handle.run_action(&action).await {
        Ok(()) => Redirect::to("/").into_response(),
        Err(error) => (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response(),
    }
}

async fn allstar_link(State(app): State<App>, Form(form): Form<AllstarLinkForm>) -> Response {
    allstar_action(&app, Action { link: Some(form.node.trim().to_string()), monitor: form.monitor.is_some(), permanent: form.permanent.is_some(), ..Action::default() }).await
}

async fn allstar_unlink(State(app): State<App>, Form(form): Form<AllstarUnlinkForm>) -> Response {
    allstar_action(&app, Action { unlink: Some(form.node.trim().to_string()), ..Action::default() }).await
}

fn routes(app: App) -> Router {
    Router::new().route("/", get(index)).route("/api/status", get(status)).route("/live", get(live)).route("/metrics", get(metrics)).route("/link", post(link)).route("/unlink", post(unlink)).route("/allstar/link", post(allstar_link)).route("/allstar/unlink", post(allstar_unlink)).with_state(app)
}

pub async fn serve(config: Config, directory: Directory, shared: Shared, commands: mpsc::Sender<Command>, allstar: Option<allstar::Handle>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let address = config.http_listen.clone().unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let app = routes(App { config, directory, shared, commands, allstar, metrics });
    let listener = tokio::net::TcpListener::bind(&address).await?;
    log::info!("Web UI on http://{address}");
    axum::serve(listener, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live::Event;
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message as ClientMessage;

    async fn next_json(socket: &mut (impl StreamExt<Item = Result<ClientMessage, tokio_tungstenite::tungstenite::Error>> + Unpin)) -> serde_json::Value {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(3), socket.next()).await.expect("a message").expect("the socket to stay open").unwrap();
            if let ClientMessage::Text(text) = message {
                return serde_json::from_str(text.as_str()).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn the_page_is_sent_the_status_and_then_each_change_and_event_as_it_happens() {
        let config: Config = toml::from_str("[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.25\nlongitude = -90.5\n[modem]\nport = \"/dev/null\"\n").unwrap();
        let directory = hosts::directory(&config);
        let shared = Shared::default();
        let (commands, _unused) = mpsc::channel(1);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = App { config, directory, shared: shared.clone(), commands, allstar: None, metrics: Arc::new(Metrics::new()) };
        tokio::spawn(async move { axum::serve(listener, routes(app)).await });
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/live")).await.unwrap();

        // The recent traffic comes first, and then the status
        let history = next_json(&mut socket).await;
        assert_eq!((history["type"].as_str(), history["transmissions"].as_array().map(Vec::len)), (Some("history"), Some(0)));
        let first = next_json(&mut socket).await;
        assert_eq!((first["type"].as_str(), first["snapshot"]["modem_connected"].as_bool()), (Some("snapshot"), Some(false)));

        // Events arrive as they happen
        shared.emit(Event::Signal { raw: 1100, dbm: Some(-95.0) });
        let event = next_json(&mut socket).await;
        assert_eq!((event["type"].as_str(), event["raw"].as_u64(), event["dbm"].as_f64()), (Some("signal"), Some(1100), Some(-95.0)));
        shared.emit(Event::Position { callsign: "N0TEST".into(), latitude: 45.5, longitude: -75.5, comment: "Hi".into() });
        assert_eq!(next_json(&mut socket).await["type"], "position");

        // And a change to the status follows
        state::update(&shared, |snapshot| snapshot.modem_connected = true);
        loop {
            let message = next_json(&mut socket).await;
            if message["type"] == "snapshot" {
                assert_eq!(message["snapshot"]["modem_connected"], true);
                break;
            }
        }
    }

    #[tokio::test]
    async fn the_page_knows_where_the_repeater_is() {
        let config: Config = toml::from_str("[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.25\nlongitude = -90.5\n[modem]\nport = \"/dev/null\"\n").unwrap();
        let directory = hosts::directory(&config);
        let app = App { config, directory, shared: Shared::default(), commands: mpsc::channel(1).0, allstar: None, metrics: Arc::new(Metrics::new()) };
        let Html(page) = index(State(app)).await;
        assert!(page.contains("latitude: 45.25, longitude: -90.5"), "the repeater's position wasn't filled in");
        assert!(!page.contains("{{"), "a placeholder was left in the page");
    }

    #[tokio::test]
    async fn prometheus_can_scrape_the_metrics_from_the_same_port() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let config: Config = toml::from_str("[station]\ncallsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.25\nlongitude = -90.5\n[modem]\nport = \"/dev/null\"\n").unwrap();
        let directory = hosts::directory(&config);
        let shared = Shared::default();
        state::update(&shared, |snapshot| snapshot.modem_connected = true);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = App { config, directory, shared, commands: mpsc::channel(1).0, allstar: None, metrics: Arc::new(Metrics::new()) };
        tokio::spawn(async move { axum::serve(listener, routes(app)).await });

        let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
        connection.write_all(b"GET /metrics HTTP/1.1\r\nHost: hotspot\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut response = String::new();
        connection.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.to_lowercase().contains("content-type: text/plain; version=0.0.4"), "{response}");
        assert!(response.contains("va3ujf_modem_connected 1"), "the status wasn't read at scrape time:\n{response}");
        assert!(response.contains("# TYPE va3ujf_transmissions_total counter"), "{response}");
    }
}
