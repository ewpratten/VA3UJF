// Arbitrates the channel between the radio and its links, and fans each stream out to where it belongs

use crate::dprs;
use crate::dstar::{pad, Frame, Header};
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
    to_member: mpsc::Sender<Frame>,
}

pub struct Router {
    inbox: mpsc::Receiver<(usize, Frame)>,
    inbox_sender: mpsc::Sender<(usize, Frame)>,

    // The radio is always the first member
    members: Vec<Member>,
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
        let mut router = Router { inbox, inbox_sender, members: Vec::new() };
        let radio = router.attach("rf", Scope::Everyone);
        (router, radio)
    }

    // Connect a link, which is then named in the transmission log by its kind
    pub fn attach(&mut self, kind: &'static str, scope: Scope) -> Port {
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
        self.members.push(Member { kind, scope, to_member });
        Port { to_router, from_router }
    }

    pub async fn run(self, mut taps: RadioTaps, shared: Shared) {
        // The router stops once every link has gone, which needs it to hold no handle on its own inbox
        let Router { mut inbox, inbox_sender, members } = self;
        drop(inbox_sender);
        let mut active: Option<(usize, Instant)> = None;
        let mut tick = interval(Duration::from_millis(250));

        loop {
            let (sender, frame) = tokio::select! {
                received = inbox.recv() => match received {
                    Some(received) => received,
                    None => return,
                },
                _ = tick.tick() => {
                    if active.is_some_and(|(_, heard)| heard.elapsed() > STREAM_TIMEOUT) {
                        active = None;
                        state::update(&shared, state::finish_transmission);
                    }
                    continue;
                }
            };

            // Local-only voice, like an announcement, is cut off by anyone else keying up
            if let (Frame::Header(_), Some((holder, _))) = (&frame, active)
                && holder != sender
                && members[holder].scope == Scope::LocalOnly
            {
                log::info!("Cutting off {} for someone keying up", members[holder].kind);
                deliver(&members, holder, &Frame::End).await;
                state::update(&shared, state::finish_transmission);
                active = None;
            }

            // Whoever keys up first holds the channel until they finish
            let holds_channel = match (&frame, active) {
                (Frame::Header(_), None) => true,
                (_, Some((holder, _))) => holder == sender,
                _ => false,
            };
            if !holds_channel {
                continue;
            }
            active = if matches!(frame, Frame::End) { None } else { Some((sender, Instant::now())) };

            // Record the transmission for the web UI
            let kind = members[sender].kind;
            match &frame {
                Frame::Header(header) => {
                    log::info!("Heard {} via {kind}", header.my_call_text());
                    state::update(&shared, |snapshot| snapshot.current = Some(Transmission { source: kind, callsign: header.my_call_text(), started_at: state::unix_now(), seconds: 0 }));
                }
                Frame::End => state::update(&shared, state::finish_transmission),
                Frame::Voice { .. } => {}
            }

            if sender == 0 && taps.observe(&frame) {
                continue;
            }
            deliver(&members, sender, &frame).await;
        }
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
    position_collector: dprs::Collector,

    // Whether the stream being heard is a request to the repeater, which goes no further than it
    request_stream: bool,
}

impl RadioTaps {
    pub fn new(position_reports: Option<mpsc::Sender<String>>, speaker: Option<Speaker>) -> Self {
        RadioTaps { position_reports, speaker, ..Default::default() }
    }

    // Returns whether the frame was for the repeater alone
    fn observe(&mut self, frame: &Frame) -> bool {
        // Position reports ride in the slow data
        match frame {
            Frame::Header(_) | Frame::End => self.position_collector.reset(),
            Frame::Voice { slow_data, .. } => {
                if let (Some(report), Some(reports)) = (self.position_collector.push(*slow_data), &self.position_reports)
                    && reports.try_send(report).is_err()
                {
                    log::warn!("APRS-IS isn't keeping up, dropping a position report");
                }
            }
        }

        // An info request is answered out loud, once whoever sent it has finished transmitting
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
        tokio::spawn(router.run(RadioTaps::new(None, Some(speaker)), Shared::default()));

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
