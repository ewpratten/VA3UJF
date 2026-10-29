// The language model that carries out what operators ask for, with tools to link, unlink and report on the node's links

use crate::allstar;
use crate::config::{Action, Station, VoiceControl};
use crate::echolink;
use crate::hosts::{self, Directory};
use crate::reflector;
use crate::speech::{module_letter, spelled, status_text};
use crate::state::{self, Shared};
use anyhow::Context;
use rig_core::client::CompletionClient;
use rig_core::completion::{Prompt, ToolDefinition};
use rig_core::http_client::{HeaderMap, HeaderValue};
use rig_core::providers::openai;
use rig_core::tool::Tool;
use serde::Deserialize;
use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::mpsc;

const MAX_TOOL_TURNS: usize = 4;

// What the tools act on
pub struct Controls {
    pub allstar: Option<allstar::Handle>,
    pub echolink: Option<echolink::Handle>,
    pub reflector_commands: mpsc::Sender<reflector::Command>,
    pub reflectors: Directory,
    pub station: Station,
    pub shared: Shared,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum Network {
    Allstar,
    Echolink,
    Reflector,
}

const NETWORKS: [&str; 3] = ["allstar", "echolink", "reflector"];

#[derive(Deserialize)]
struct LinkArguments {
    network: Network,
    target: String,
    module: Option<String>,
    #[serde(default)]
    monitor: bool,
    #[serde(default)]
    permanent: bool,
}

#[derive(Deserialize)]
struct UnlinkArguments {
    network: Network,
    target: Option<String>,
}

#[derive(Deserialize)]
struct NoArguments {}

struct LinkTool(Arc<Controls>);
struct UnlinkTool(Arc<Controls>);
struct StatusTool(Arc<Controls>);

// Failures are results the model can read out, rather than errors that end the conversation
fn outcome(result: anyhow::Result<String>) -> Result<String, Infallible> {
    Ok(result.unwrap_or_else(|error| format!("That didn't work: {error:#}")))
}

impl Controls {
    async fn link(&self, arguments: LinkArguments) -> anyhow::Result<String> {
        let target = arguments.target.trim().to_string();
        match arguments.network {
            Network::Allstar => {
                let handle = self.allstar.as_ref().context("AllStar isn't set up on this node")?;
                handle.run_action(&Action { link: Some(target.clone()), monitor: arguments.monitor, permanent: arguments.permanent, ..Action::default() }).await?;
                Ok(format!("Linking to AllStar node {target}."))
            }
            Network::Echolink => {
                let handle = self.echolink.as_ref().context("EchoLink isn't set up on this node")?;
                handle.link(&target).await?;
                Ok(format!("Linking to EchoLink station {target}."))
            }
            Network::Reflector => {
                let module = module_letter(&arguments.module.unwrap_or_default()).context("a reflector link needs a module, which is a letter")?;
                let reflector = hosts::find(&self.reflectors, &target).with_context(|| format!("{target} isn't a reflector this node knows"))?;
                self.reflector_commands.send(reflector::Command::Link { name: reflector.name.clone(), module }).await.map_err(|_| anyhow::anyhow!("the reflector client isn't running"))?;
                Ok(format!("Linking to reflector {} module {module}.", reflector.name))
            }
        }
    }

    async fn unlink(&self, arguments: UnlinkArguments) -> anyhow::Result<String> {
        let target = arguments.target.map(|target| target.trim().to_string()).filter(|target| !target.is_empty()).unwrap_or_else(|| "all".to_string());
        match arguments.network {
            Network::Allstar => {
                let handle = self.allstar.as_ref().context("AllStar isn't set up on this node")?;
                handle.run_action(&Action { unlink: Some(target.clone()), ..Action::default() }).await?;
            }
            Network::Echolink => self.echolink.as_ref().context("EchoLink isn't set up on this node")?.unlink(&target).await?,
            Network::Reflector => self.reflector_commands.send(reflector::Command::Unlink).await.map_err(|_| anyhow::anyhow!("the reflector client isn't running"))?,
        }
        Ok(format!("Unlinking {target}."))
    }
}

fn network_schema() -> serde_json::Value {
    json!({ "type": "string", "enum": NETWORKS, "description": "allstar links nodes by number, echolink links stations by callsign or number, reflector links a D-STAR reflector such as DCS405" })
}

impl Tool for LinkTool {
    const NAME: &'static str = "link";
    type Error = Infallible;
    type Args = LinkArguments;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Link this node to an AllStar node, an EchoLink station, or a D-STAR reflector".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "network": network_schema(),
                    "target": { "type": "string", "description": "The AllStar node number, EchoLink callsign or number, or reflector name, with no spaces" },
                    "module": { "type": "string", "description": "The reflector's module, as a letter like A. Only for reflectors" },
                    "monitor": { "type": "boolean", "description": "Listen only. Only for AllStar" },
                    "permanent": { "type": "boolean", "description": "Redial if the link drops. Only for AllStar, and only when asked" }
                },
                "required": ["network", "target"]
            }),
        }
    }

    async fn call(&self, arguments: Self::Args) -> Result<Self::Output, Self::Error> {
        outcome(self.0.link(arguments).await)
    }
}

impl Tool for UnlinkTool {
    const NAME: &'static str = "unlink";
    type Error = Infallible;
    type Args = UnlinkArguments;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition {
            name: Self::NAME.to_string(),
            description: "Drop one link, or all of a network's links".to_string(),
            parameters: json!({
                "type": "object",
                "properties": {
                    "network": network_schema(),
                    "target": { "type": "string", "description": "What to drop, or \"all\" for everything on the network. A reflector has only one link, so it needs no target" }
                },
                "required": ["network"]
            }),
        }
    }

    async fn call(&self, arguments: Self::Args) -> Result<Self::Output, Self::Error> {
        outcome(self.0.unlink(arguments).await)
    }
}

impl Tool for StatusTool {
    const NAME: &'static str = "status";
    type Error = Infallible;
    type Args = NoArguments;
    type Output = String;

    async fn definition(&self, _prompt: String) -> ToolDefinition {
        ToolDefinition { name: Self::NAME.to_string(), description: "What this node is linked to right now".to_string(), parameters: json!({ "type": "object", "properties": {} }) }
    }

    async fn call(&self, _arguments: Self::Args) -> Result<Self::Output, Self::Error> {
        Ok(status_text(&self.0.station, &state::read(&self.0.shared)))
    }
}

// Numbers and callsigns are read out one character at a time, the way they're said on air
pub fn speakable(text: &str) -> String {
    text.split_whitespace()
        .map(|word| {
            let core = word.trim_matches(|character: char| !character.is_alphanumeric());
            match word.find(core) {
                Some(start) if core.chars().any(|character| character.is_ascii_digit()) => format!("{}{}{}", &word[..start], spelled(core), &word[start + core.len()..]),
                _ => word.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub struct Agent {
    client: openai::CompletionsClient,
    model: String,
    controls: Arc<Controls>,
}

impl Agent {
    pub async fn connect(config: &VoiceControl, controls: Controls) -> anyhow::Result<Self> {
        let token = tokio::fs::read_to_string(&config.token_file).await.with_context(|| format!("couldn't read {}", config.token_file.display()))?;
        let token = token.trim();
        anyhow::ensure!(!token.is_empty(), "{} is empty", config.token_file.display());
        let mut headers = HeaderMap::new();
        headers.insert("cf-aig-authorization", HeaderValue::from_str(&format!("Bearer {token}")).context("the gateway token isn't a valid header")?);
        let base_url = format!("https://gateway.ai.cloudflare.com/v1/{}/{}/workers-ai/v1", config.account_id, config.gateway_id);
        let client = openai::Client::builder().api_key(token).base_url(base_url).http_headers(headers).build()?.completions_api();
        Ok(Agent { client, model: config.model.clone(), controls: Arc::new(controls) })
    }

    // What to do about what an operator said, answered in a sentence that's fit to be spoken
    pub async fn respond(&self, request: &str) -> anyhow::Result<String> {
        let controls = &self.controls;
        let mut networks = vec!["D-STAR reflectors"];
        if controls.allstar.is_some() {
            networks.push("AllStar nodes");
        }
        if controls.echolink.is_some() {
            networks.push("EchoLink stations");
        }
        let preamble = format!(
            "You are the voice of {callsign}, an amateur radio D-STAR repeater. Operators talk to you over the air, and what they say is transcribed by speech recognition, \
             so expect mistakes with callsigns and digits. You can link this repeater to {networks}, drop links, and say what is linked. \
             Use the tools to do what is asked, and never claim to have done something you haven't. If the request is unclear or isn't about managing links, say so and ask again. \
             Your reply is read aloud over the radio, so answer in one short plain sentence, with no markup. \
             Write node numbers, reflector names and callsigns with a space between every character, like 5 4 2 0 7 or D C S 4 0 5. \
             Right now: {status}",
            callsign = spelled(&controls.station.callsign),
            networks = networks.join(", "),
            status = status_text(&controls.station, &state::read(&controls.shared)),
        );
        let agent = self.client.agent(&self.model).preamble(&preamble).tool(LinkTool(controls.clone())).tool(UnlinkTool(controls.clone())).tool(StatusTool(controls.clone())).build();
        let reply = agent.prompt(request).max_turns(MAX_TOOL_TURNS).await?;
        Ok(speakable(reply.trim()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_in_a_reply_are_spelled_out_and_the_rest_is_left_alone() {
        assert_eq!(speakable("Linking to node 54207, now."), "Linking to node 5 4 2 0 7, now.");
        assert_eq!(speakable("Linked to DCS405."), "Linked to D C S 4 0 5.");
        assert_eq!(speakable("All done"), "All done");
    }

    // Talks to the real gateway, so it only runs where VA3UJF_GATEWAY_TOKEN_FILE and VA3UJF_GATEWAY_ACCOUNT_ID say how
    #[tokio::test]
    async fn the_model_uses_its_tools_through_the_gateway() {
        let (Some(token_file), Ok(account_id)) = (std::env::var_os("VA3UJF_GATEWAY_TOKEN_FILE"), std::env::var("VA3UJF_GATEWAY_ACCOUNT_ID")) else { return };
        let config: VoiceControl = toml::from_str(&format!("operators = [\"N0CALL\"]\naccount_id = {account_id:?}\ntoken_file = {:?}", token_file.to_string_lossy())).unwrap();
        let station: Station = toml::from_str("callsign = \"N0CALL\"\nmodule = \"B\"\nfrequency_mhz = 446.5\nlatitude = 45.0\nlongitude = -90.0").unwrap();
        let (reflector_commands, mut commands) = mpsc::channel(4);
        let reflector: crate::config::Reflector = toml::from_str("name = \"DCS405\"\nprotocol = \"dcs\"\naddress = \"192.0.2.1\"").unwrap();
        let reflectors = std::sync::Arc::new(std::sync::RwLock::new(vec![reflector]));
        let agent = Agent::connect(&config, Controls { allstar: None, echolink: None, reflector_commands, reflectors, station, shared: Shared::default() }).await.unwrap();

        // A request the node can carry out is passed to the tool, and one it can't is explained
        agent.respond("Link to D C S four oh five, module alpha.").await.unwrap();
        assert!(matches!(commands.try_recv(), Ok(reflector::Command::Link { name, module: 'A' }) if name == "DCS405"));
        assert!(!agent.respond("Connect to All Star node five four three two one.").await.unwrap().is_empty());
    }
}
