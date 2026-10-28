// Arbitrates the channel between the radio and its links, and fans each stream out to where it belongs

use crate::ack::AckRequest;
use crate::codec::fec;
use crate::dprs;
use crate::dstar::{pad, Frame, Header};
use crate::live::Event;
use crate::meter;
use crate::slowdata::{Decoded, Decoder};
use crate::speech::{Announcement, Speaker};
use crate::state::{self, Shared, Transmission};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};

// Release the channel if a stream goes quiet without an end marker
const STREAM_TIMEOUT: Duration = Duration::from_secs(2);
const PORT_DEPTH: usize = 256;

// Who a link's voice reaches, and whose voice reaches it
#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Scope {
    // Hears the radio and every other link, and is heard by all of them
    Everyone,

    // Hears nothing, and is heard on the radio only. For announcements
    LocalOnly,
}

// A link's end of its connection to the router
pub struct Port {
    pub to_router: mpsc::Sender<Frame>,
    pub from_router: mpsc::Receiver<Frame>,
}

struct Member {
    kind: &'static str,
    scope: Scope,

    // Whether its transmissions are a station to list as a peer
    tracked: bool,
    to_member: mpsc::Sender<Frame>,
}

pub struct Router {
    inbox: mpsc::Receiver<(usize, Frame)>,
    inbox_sender: mpsc::Sender<(usize, Frame)>,

    // The radio is always the first member
    members: Vec<Member>,

    // Where the voice of every transmission goes to be measured, if anything is measuring
    meter: Option<mpsc::Sender<meter::Input>>,
}

async fn deliver(members: &[Member], sender: usize, frame: &Frame) {
    let radio_only = members[sender].scope == Scope::LocalOnly;
    for (index, member) in members.iter().enumerate() {
        let reaches = index != sender && member.scope == Scope::Everyone && (!radio_only || index == 0);
        if reaches {
            let _ = member.to_member.send(frame.clone()).await;
        }
    }
}

impl Router {
    // A router with the radio attached, and the radio's port
    pub fn new() -> (Self, Port) {
        let (inbox_sender, inbox) = mpsc::channel(PORT_DEPTH);
        let mut router = Router { inbox, inbox_sender, members: Vec::new(), meter: None };
        let radio = router.attach("rf", Scope::Everyone);
        (router, radio)
    }

    pub fn set_meter(&mut self, meter: mpsc::Sender<meter::Input>) {
        self.meter = Some(meter);
    }

    // Connect a link, which is then named in the transmission log by its kind
    pub fn attach(&mut self, kind: &'static str, scope: Scope) -> Port {
        self.attach_member(kind, scope, scope == Scope::Everyone)
    }

    // Connect a link that speaks for parties of its own, which it reports itself, rather than being one
    pub fn attach_bridge(&mut self, kind: &'static str) -> Port {
        self.attach_member(kind, Scope::Everyone, false)
    }

    fn attach_member(&mut self, kind: &'static str, scope: Scope, tracked: bool) -> Port {
        let index = self.members.len();
        let (to_router, mut from_member) = mpsc::channel(PORT_DEPTH);
        let (to_member, from_router) = mpsc::channel(PORT_DEPTH);
        let inbox = self.inbox_sender.clone();
        tokio::spawn(async move {
            while let Some(frame) = from_member.recv().await {
                if inbox.send((index, frame)).await.is_err() {
                    return;
                }
            }
        });
        self.members.push(Member { kind, scope, tracked, to_member });
        Port { to_router, from_router }
    }

    pub async fn run(self, mut taps: RadioTaps, shared: Shared) {
        // The router stops once every link has gone, which needs it to hold no handle on its own inbox
        let Router { mut inbox, inbox_sender, members, meter } = self;
        drop(inbox_sender);
        let mut events = shared.subscribe_events();
        let mut decoders: Vec<Decoder> = members.iter().map(|_| Decoder::default()).collect();
        let mut quality = RfQuality::default();
        let mut active: Option<Active> = None;
        let mut tick = interval(Duration::from_millis(250));

        loop {
            let (sender, frame) = tokio::select! {
                received = inbox.recv() => match received {
                    Some(received) => received,
                    None => return,
                },
                event = events.recv() => {
                    // How well the radio is hearing whoever is on the air
                    if let Ok(Event::Signal { dbm, .. }) = event
                        && active.as_ref().is_some_and(|holder| holder.member == 0)
                    {
                        quality.signal(dbm);
                    }
                    continue;
                }
                _ = tick.tick() => {
                    if active.as_ref().is_some_and(|holder| holder.heard.elapsed() > STREAM_TIMEOUT) {
                        active = None;
                        state::end_transmission(&shared);
                    }
                    continue;
                }
            };

            // Local-only voice, like an announcement, is cut off by anyone else keying up
            if let (Frame::Header(_), Some(holder)) = (&frame, active.as_ref().map(|holder| holder.member))
                && holder != sender
                && members[holder].scope == Scope::LocalOnly
            {
                log::info!("Cutting off {} for someone keying up", members[holder].kind);
                deliver(&members, holder, &Frame::End).await;
                state::end_transmission(&shared);
                active = None;
            }

            // Whoever keys up first holds the channel until they finish
            let holds_channel = match (&frame, &active) {
                (Frame::Header(_), None) => true,
                (_, Some(holder)) => holder.member == sender,
                _ => false,
            };
            if !holds_channel {
                continue;
            }

            let kind = members[sender].kind;
            let tracked = members[sender].tracked;
            let metered = members[sender].scope == Scope::Everyone;
            if let Frame::Header(header) = &frame {
                let callsign = header.my_call_text();
                log::info!("Heard {callsign} via {kind}");
                decoders[sender].reset();
                if sender == 0 {
                    quality = RfQuality::default();
                }
                state::update(&shared, |snapshot| {
                    snapshot.current = Some(Transmission { source: kind, callsign: callsign.clone(), started_at: state::unix_now(), seconds: 0, tracked });
                    if tracked {
                        state::peer_keyed_up(snapshot, &callsign, kind);
                    }
                });
                active = Some(Active { member: sender, heard: Instant::now(), header: *header, callsign });
            }
            let Some(holder) = active.as_mut() else { continue };
            holder.heard = Instant::now();
            let callsign = holder.callsign.clone();

            // Voice moving through the repeater is measured for how loud it is. If the meter can't keep up, it misses some
            if let (true, Some(meter)) = (metered, &meter) {
                let _ = match &frame {
                    Frame::Header(_) => meter.try_send(meter::Input::Start),
                    Frame::Voice { ambe, .. } => meter.try_send(meter::Input::Frame { source: kind, callsign: callsign.clone(), ambe: *ambe }),
                    Frame::End => meter.try_send(meter::Input::End),
                };
            }

            // What's in the slow data, and how clean the voice is
            if let Frame::Voice { ambe, slow_data, .. } = &frame {
                if sender == 0 {
                    quality.voice(ambe);
                }
                if tracked && let Some(decoded) = decoders[sender].push(*slow_data) {
                    publish_decoded(&shared, &mut taps, sender == 0, &callsign, decoded);
                }
            }

            // An info request is answered out loud, and goes no further than the repeater
            let for_repeater_only = sender == 0 && taps.observe(&frame);

            if matches!(frame, Frame::End) {
                let finished = active.take();
                state::end_transmission(&shared);
                if let (0, false, Some(finished)) = (sender, for_repeater_only, finished) {
                    report_quality(&shared, &taps, &finished, &quality);
                }
            }
            if for_repeater_only {
                continue;
            }
            deliver(&members, sender, &frame).await;
        }
    }
}

// The holder of the channel
struct Active {
    member: usize,
    heard: Instant,
    header: Header,
    callsign: String,
}

// How well the radio heard an RF transmission
#[derive(Default)]
struct RfQuality {
    frames: u32,
    errors: u32,
    dbm_total: f32,
    dbm_readings: u32,
}

impl RfQuality {
    fn voice(&mut self, ambe: &[u8; 9]) {
        self.frames += 1;
        if *ambe != fec::SILENCE {
            self.errors += fec::bit_errors(ambe);
        }
    }

    fn signal(&mut self, dbm: Option<f32>) {
        if let Some(dbm) = dbm {
            self.dbm_total += dbm;
            self.dbm_readings += 1;
        }
    }

    // Each frame of voice is 48 bits of the ones the error correction looks at
    fn ber_percent(&self) -> f32 {
        if self.frames == 0 { 0.0 } else { self.errors as f32 * 100.0 / (self.frames * 48) as f32 }
    }

    fn dbm(&self) -> Option<f32> {
        (self.dbm_readings > 0).then(|| self.dbm_total / self.dbm_readings as f32)
    }
}

// Pass on what a transmission's slow data said
fn publish_decoded(shared: &Shared, taps: &mut RadioTaps, from_radio: bool, callsign: &str, decoded: Decoded) {
    let (kind, text) = match decoded {
        Decoded::Text(text) => {
            state::update(shared, |snapshot| {
                if let Some(peer) = state::peer_mut(snapshot, callsign) {
                    peer.message = text.clone();
                }
            });
            ("text", text)
        }
        Decoded::Gps(line) => {
            if let Some(fix) = dprs::parse_fix(&line) {
                shared.emit(Event::Position { callsign: callsign.to_string(), latitude: fix.latitude, longitude: fix.longitude, comment: fix.comment.clone() });
                state::update(shared, |snapshot| {
                    if let Some(peer) = state::peer_mut(snapshot, callsign) {
                        peer.fix = Some(fix);
                    }
                });
            }

            // Only what's heard directly over RF is the IGate's to pass on
            if from_radio {
                taps.gps_report(&line);
            }
            ("gps", line)
        }
        Decoded::Other { kind, bytes } => (kind, bytes.iter().map(|byte| format!("{byte:02X}")).collect::<Vec<_>>().join(" ")),
    };
    shared.emit(Event::SlowData { callsign: callsign.to_string(), kind, text });
}

// Tell the web UI how an RF transmission went, and have the repeater say so to whoever sent it
fn report_quality(shared: &Shared, taps: &RadioTaps, finished: &Active, quality: &RfQuality) {
    let (ber_percent, dbm, seconds) = (quality.ber_percent(), quality.dbm(), quality.frames as f32 / 50.0);
    state::update(shared, |snapshot| {
        if let Some(peer) = state::peer_mut(snapshot, &finished.callsign) {
            (peer.ber_percent, peer.dbm) = (Some(ber_percent), dbm);
        }
    });
    shared.emit(Event::Quality { callsign: finished.callsign.clone(), ber_percent, dbm, seconds });
    log::info!("{} was heard with BER {ber_percent:.1}%{}", finished.callsign, dbm.map(|dbm| format!(", {dbm:.0} dBm")).unwrap_or_default());
    if let Some(acks) = &taps.acks {
        let _ = acks.try_send(AckRequest { your_call: finished.header.my_call, ber_percent, dbm });
    }
}

// Sent as the "your call" of a transmission, this asks the repeater to describe itself
fn is_info_request(header: &Header) -> bool {
    header.your_call == pad::<8>("       I")
}

// What the repeater does with what it hears directly over RF, before it goes anywhere else
#[derive(Default)]
pub struct RadioTaps {
    pub position_reports: Option<mpsc::Sender<String>>,
    pub speaker: Option<Speaker>,
    pub acks: Option<mpsc::Sender<AckRequest>>,

    // Whether the stream being heard is a request to the repeater, which goes no further than it
    request_stream: bool,
}

impl RadioTaps {
    pub fn new(position_reports: Option<mpsc::Sender<String>>, speaker: Option<Speaker>, acks: Option<mpsc::Sender<AckRequest>>) -> Self {
        RadioTaps { position_reports, speaker, acks, request_stream: false }
    }

    // A position report heard over RF, on its way to APRS-IS
    fn gps_report(&mut self, report: &str) {
        if let Some(reports) = &self.position_reports
            && reports.try_send(report.to_string()).is_err()
        {
            log::warn!("APRS-IS isn't keeping up, dropping a position report");
        }
    }

    // An info request is answered out loud, once whoever sent it has finished transmitting. Returns whether the frame was for the repeater alone
    fn observe(&mut self, frame: &Frame) -> bool {
        if let Frame::Header(header) = frame {
            self.request_stream = is_info_request(header);
            if self.request_stream {
                log::info!("{} asked for repeater info", header.my_call_text());
                if let Some(speaker) = &self.speaker {
                    let _ = speaker.try_send(Announcement::Status);
                }
            }
        }
        self.request_stream
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn header(your_call: &str) -> Header {
        Header { flags: [0; 3], repeater_2: pad("N0CALL G"), repeater_1: pad("N0CALL B"), your_call: pad(your_call), my_call: pad("N0TEST"), my_suffix: pad("TEST") }
    }

    async fn transmit(frames: &mpsc::Sender<Frame>, your_call: &str) {
        frames.send(Frame::Header(header(your_call))).await.unwrap();
        frames.send(Frame::Voice { sequence: 0, ambe: [1; 9], slow_data: [2; 3] }).await.unwrap();
        frames.send(Frame::End).await.unwrap();
    }

    // Whether a transmission starts, skipping the rest of any earlier one
    async fn heard_header(port: &mut Port) -> bool {
        while let Ok(Some(frame)) = tokio::time::timeout(Duration::from_secs(2), port.from_router.recv()).await {
            if matches!(frame, Frame::Header(_)) {
                return true;
            }
        }
        false
    }

    async fn heard_nothing(port: &mut Port) -> bool {
        tokio::time::timeout(Duration::from_millis(300), port.from_router.recv()).await.is_err()
    }

    #[tokio::test]
    async fn an_info_request_is_answered_aloud_and_goes_no_further() {
        let (mut router, radio) = Router::new();
        let mut link = router.attach("test", Scope::Everyone);
        let (speaker, mut announcements) = mpsc::channel(4);
        tokio::spawn(router.run(RadioTaps::new(None, Some(speaker), None), Shared::default()));

        // The request is spoken back, and none of it reaches the link
        transmit(&radio.to_router, "       I").await;
        assert_eq!(announcements.recv().await, Some(Announcement::Status));
        assert!(heard_nothing(&mut link).await, "the request leaked to a link");

        // An ordinary transmission right after it still does
        transmit(&radio.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut link).await);
        assert!(announcements.try_recv().is_err(), "an ordinary transmission was taken for an info request");
    }

    #[tokio::test]
    async fn links_hear_the_radio_and_each_other_but_never_themselves() {
        let (mut router, mut radio) = Router::new();
        let mut first = router.attach("first", Scope::Everyone);
        let mut second = router.attach("second", Scope::Everyone);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        transmit(&first.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut radio).await && heard_header(&mut second).await);
        assert!(heard_nothing(&mut first).await, "a link heard its own transmission");

        transmit(&radio.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut first).await && heard_header(&mut second).await);
    }

    #[tokio::test]
    async fn a_local_only_link_is_heard_on_the_radio_alone_and_hears_nothing() {
        let (mut router, mut radio) = Router::new();
        let mut link = router.attach("link", Scope::Everyone);
        let mut announcer = router.attach("announcement", Scope::LocalOnly);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        transmit(&announcer.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut radio).await);
        assert!(heard_nothing(&mut link).await, "an announcement reached a link");

        transmit(&radio.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut link).await);
        assert!(heard_nothing(&mut announcer).await, "the announcer was sent a transmission");
    }

    #[tokio::test]
    async fn an_announcement_is_cut_off_when_anyone_else_keys_up() {
        let (mut router, mut radio) = Router::new();
        let link = router.attach("link", Scope::Everyone);
        let announcer = router.attach("announcement", Scope::LocalOnly);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        announcer.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();
        announcer.to_router.send(Frame::Voice { sequence: 0, ambe: [1; 9], slow_data: [2; 3] }).await.unwrap();
        assert!(heard_header(&mut radio).await);
        link.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();

        // The announcement is ended for the radio, then the conversation comes through, and the rest of the announcement is dropped
        let mut heard = Vec::new();
        while heard.len() < 3 {
            match tokio::time::timeout(Duration::from_secs(2), radio.from_router.recv()).await {
                Ok(Some(frame)) => heard.push(frame),
                _ => break,
            }
        }
        assert!(matches!(heard.as_slice(), [Frame::Voice { .. }, Frame::End, Frame::Header(_)]), "{heard:?}");
        announcer.to_router.send(Frame::Voice { sequence: 1, ambe: [1; 9], slow_data: [2; 3] }).await.unwrap();
        assert!(heard_nothing(&mut radio).await, "the announcement carried on over the conversation");
    }

    async fn rf_transmission(radio: &Port, frames: Vec<Frame>) {
        radio.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();
        for frame in frames {
            radio.to_router.send(frame).await.unwrap();
        }
        radio.to_router.send(Frame::End).await.unwrap();
    }

    async fn event(events: &mut tokio::sync::broadcast::Receiver<Event>, wanted: impl Fn(&Event) -> bool) -> Event {
        loop {
            let event = tokio::time::timeout(Duration::from_secs(2), events.recv()).await.expect("an event").unwrap();
            if wanted(&event) {
                return event;
            }
        }
    }

    #[tokio::test]
    async fn a_station_that_keys_up_is_a_peer_that_lights_up_while_it_talks() {
        let (router, radio) = Router::new();
        let shared = Shared::default();
        tokio::spawn(router.run(RadioTaps::default(), shared.clone()));

        radio.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();
        for _ in 0..50 {
            if state::read(&shared).peers.first().is_some_and(|peer| peer.talking) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let talking = state::read(&shared).peers;
        assert_eq!((talking[0].callsign.as_str(), talking[0].via, talking[0].talking), ("N0TEST/TEST", "rf", true));

        radio.to_router.send(Frame::End).await.unwrap();
        for _ in 0..50 {
            if state::read(&shared).peers.first().is_some_and(|peer| !peer.talking) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let finished = state::read(&shared).peers;
        assert_eq!((finished.len(), finished[0].talking, finished[0].transmissions), (1, false, 1));
    }

    #[tokio::test]
    async fn slow_data_text_and_positions_reach_the_peer_and_the_event_stream() {
        let (mut router, radio) = Router::new();
        let _link = router.attach("link", Scope::Everyone);
        let shared = Shared::default();
        let mut events = shared.subscribe_events();
        let (reports, mut gated) = mpsc::channel(4);
        tokio::spawn(router.run(RadioTaps::new(Some(reports), None, None), shared.clone()));

        let report = "N0TEST>API282,DSTAR*:!4530.00N/07545.60W>Hi\r";
        let mut frames: Vec<Frame> = crate::dprs::testing::slow_data_frames(report).into_iter().map(|slow_data| Frame::Voice { sequence: 0, ambe: [1; 9], slow_data }).collect();
        frames.extend(crate::dstar::build_slow_data_blocks("Hello").into_iter().map(|slow_data| Frame::Voice { sequence: 0, ambe: [1; 9], slow_data }));
        rf_transmission(&radio, frames).await;

        let Event::Position { callsign, latitude, .. } = event(&mut events, |event| matches!(event, Event::Position { .. })).await else { unreachable!() };
        assert_eq!((callsign.as_str(), latitude), ("N0TEST/TEST", 45.5));
        let Event::SlowData { text, .. } = event(&mut events, |event| matches!(event, Event::SlowData { kind: "text", .. })).await else { unreachable!() };
        assert!(text.starts_with("Hel"), "{text}");

        // The report is also on its way to APRS-IS, and the peer remembers where it is and what it said
        assert!(gated.recv().await.unwrap().starts_with("N0TEST>API282"));
        let peers = state::read(&shared).peers;
        assert_eq!((peers[0].fix.as_ref().map(|fix| fix.latitude), peers[0].message.as_str()), (Some(45.5), "Hello"));
    }

    #[tokio::test]
    async fn an_rf_transmission_is_measured_and_its_sender_is_acknowledged() {
        let (router, radio) = Router::new();
        let shared = Shared::default();
        let mut events = shared.subscribe_events();
        let (acks, mut requests) = mpsc::channel(4);
        tokio::spawn(router.run(RadioTaps::new(None, None, Some(acks)), shared.clone()));

        // Ten clean frames of voice, and one that lost a bit on the way
        let clean = fec::testing::frame_for(0x5A3, 0x2C7);
        let mut frames: Vec<Frame> = (0..10).map(|_| Frame::Voice { sequence: 0, ambe: clean, slow_data: [0; 3] }).collect();
        frames.push(Frame::Voice { sequence: 0, ambe: fec::testing::damaged(clean, 0), slow_data: [0; 3] });
        shared.emit(Event::Signal { raw: 1, dbm: Some(-90.0) });
        rf_transmission(&radio, frames).await;

        let Event::Quality { callsign, ber_percent, seconds, .. } = event(&mut events, |event| matches!(event, Event::Quality { .. })).await else { unreachable!() };
        assert_eq!(callsign, "N0TEST/TEST");
        assert!((ber_percent - 100.0 / 528.0).abs() < 0.01 && seconds > 0.2, "{ber_percent}% over {seconds}s");
        let request = requests.recv().await.unwrap();
        assert_eq!(request.your_call, pad("N0TEST"));
    }

    #[tokio::test]
    async fn an_info_request_is_measured_but_not_acknowledged() {
        let (router, radio) = Router::new();
        let (acks, mut requests) = mpsc::channel(4);
        let (speaker, _announcements) = mpsc::channel(4);
        tokio::spawn(router.run(RadioTaps::new(None, Some(speaker), Some(acks)), Shared::default()));

        transmit(&radio.to_router, "       I").await;
        assert!(tokio::time::timeout(Duration::from_millis(300), requests.recv()).await.is_err(), "an info request was acknowledged");
        transmit(&radio.to_router, "CQCQCQ").await;
        assert!(requests.recv().await.is_some());
    }

    #[tokio::test]
    async fn finished_voice_is_reported_for_the_flow_chart_but_announcements_are_not() {
        let (mut router, radio) = Router::new();
        let link = router.attach("reflector", Scope::Everyone);
        let announcer = router.attach("announcement", Scope::LocalOnly);
        let shared = Shared::default();
        let mut events = shared.subscribe_events();
        tokio::spawn(router.run(RadioTaps::default(), shared.clone()));

        transmit(&announcer.to_router, "CQCQCQ").await;
        transmit(&radio.to_router, "CQCQCQ").await;
        transmit(&link.to_router, "CQCQCQ").await;
        let mut sources = Vec::new();
        while sources.len() < 2 {
            let Event::Transmission { source, callsign, .. } = event(&mut events, |event| matches!(event, Event::Transmission { .. })).await else { unreachable!() };
            assert_eq!(callsign, "N0TEST/TEST");
            sources.push(source);
        }
        assert_eq!(sources, ["rf", "reflector"]);

        // The history keeps both, for a page that connects later
        let history = state::read(&shared).history;
        assert_eq!(history.iter().map(|transmission| transmission.source).collect::<Vec<_>>(), ["rf", "reflector"]);
    }

    #[tokio::test]
    async fn the_voice_of_real_traffic_is_sent_to_the_meter_but_announcements_are_not() {
        let (mut router, radio) = Router::new();
        let announcer = router.attach("announcement", Scope::LocalOnly);
        let (meter, mut measured) = mpsc::channel(64);
        router.set_meter(meter);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        transmit(&announcer.to_router, "CQCQCQ").await;
        transmit(&radio.to_router, "CQCQCQ").await;
        let mut heard = Vec::new();
        while heard.len() < 4 {
            match tokio::time::timeout(Duration::from_secs(2), measured.recv()).await {
                Ok(Some(input)) => heard.push(match input {
                    meter::Input::Start => "start".to_string(),
                    meter::Input::Frame { source, callsign, .. } => format!("{source} {callsign}"),
                    meter::Input::End => "end".to_string(),
                }),
                _ => break,
            }
        }
        assert_eq!(heard, ["start", "rf N0TEST/TEST", "end"]);
    }

    #[tokio::test]
    async fn a_bridge_is_not_listed_as_a_peer_but_still_carries_voice() {
        let (mut router, mut radio) = Router::new();
        let bridge = router.attach_bridge("bridge");
        let shared = Shared::default();
        tokio::spawn(router.run(RadioTaps::default(), shared.clone()));

        transmit(&bridge.to_router, "CQCQCQ").await;
        assert!(heard_header(&mut radio).await);
        assert!(state::read(&shared).peers.is_empty(), "the bridge was listed as a station");
    }

    #[tokio::test]
    async fn whoever_keys_up_first_holds_the_channel() {
        let (mut router, mut radio) = Router::new();
        let first = router.attach("first", Scope::Everyone);
        let second = router.attach("second", Scope::Everyone);
        tokio::spawn(router.run(RadioTaps::default(), Shared::default()));

        first.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();
        assert!(heard_header(&mut radio).await);
        second.to_router.send(Frame::Header(header("CQCQCQ"))).await.unwrap();
        assert!(heard_nothing(&mut radio).await, "a second transmission talked over the first");
    }
}
