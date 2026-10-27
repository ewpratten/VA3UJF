// Minimal web UI for watching the hotspot and controlling its reflector link

use crate::allstar;
use crate::config::{Action, Config};
use crate::hosts::{self, Directory};
use crate::reflector::Command;
use crate::state::{self, Shared};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use serde::Deserialize;
use tokio::sync::mpsc;

const PAGE: &str = include_str!("page.html");

#[derive(Clone)]
struct App {
    config: Config,
    directory: Directory,
    shared: Shared,
    commands: mpsc::Sender<Command>,
}

// Checkboxes are only sent when ticked
#[derive(Deserialize)]
struct AllstarLinkForm {
    node: String,
    monitor: Option<String>,
    permanent: Option<String>,
    echolink: Option<String>,
}

#[derive(Deserialize)]
struct AllstarUnlinkForm {
    node: String,
    echolink: Option<String>,
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
            .replace("{{frequency}}", &format!("{:.4}", station.frequency_mhz))
            .replace("{{module}}", &escape(&station.module))
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
    let Some(allstar_config) = &app.config.allstar else { return (StatusCode::NOT_FOUND, "AllStar isn't configured").into_response() };
    match allstar::step(&action) {
        Err(error) => (StatusCode::BAD_REQUEST, format!("{error:#}")).into_response(),
        Ok(_) => match allstar::run_action(allstar_config.node, &action).await {
            Ok(()) => Redirect::to("/").into_response(),
            Err(error) => (StatusCode::BAD_GATEWAY, format!("{error:#}")).into_response(),
        },
    }
}

async fn allstar_link(State(app): State<App>, Form(form): Form<AllstarLinkForm>) -> Response {
    allstar_action(&app, Action { link: Some(form.node.trim().to_string()), monitor: form.monitor.is_some(), permanent: form.permanent.is_some(), echolink: form.echolink.is_some(), ..Action::default() }).await
}

async fn allstar_unlink(State(app): State<App>, Form(form): Form<AllstarUnlinkForm>) -> Response {
    allstar_action(&app, Action { unlink: Some(form.node.trim().to_string()), echolink: form.echolink.is_some(), ..Action::default() }).await
}

pub async fn serve(config: Config, directory: Directory, shared: Shared, commands: mpsc::Sender<Command>) -> anyhow::Result<()> {
    let address = config.http_listen.clone().unwrap_or_else(|| "0.0.0.0:8080".to_string());
    let app = Router::new().route("/", get(index)).route("/api/status", get(status)).route("/link", post(link)).route("/unlink", post(unlink)).route("/allstar/link", post(allstar_link)).route("/allstar/unlink", post(allstar_unlink)).with_state(App { config, directory, shared, commands });
    let listener = tokio::net::TcpListener::bind(&address).await?;
    log::info!("Web UI on http://{address}");
    axum::serve(listener, app).await?;
    Ok(())
}
