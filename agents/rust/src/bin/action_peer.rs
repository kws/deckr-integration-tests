//! Stdio drives application behavior; public Deckr APIs own wire and authority.
use deckr::action_runtime::{
    ActionAvailability, ActionAvailabilityPayload, ActionMessage, ActionModel,
};
use deckr::endpoint::EndpointAddress;
use deckr::lanes::DeckrMessage;
use deckr::messaging::MessageAuthority;
use deckr::nats::{DeckrRuntime, NatsMessageTransport, NatsStateStore};
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::StateStorePolicy;
use deckr::{Error, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

type Connection = ServiceConnection<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type Incoming = ServiceIncoming<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type View = ActionAvailability<NatsStateStore, NatsStateStore, NatsStateStore>;
const FAMILY: &str = "action_availability";

#[derive(Default)]
struct Observations {
    received: Vec<Value>,
    connections: BTreeMap<String, Connection>,
    views: Vec<View>,
    errors: Vec<String>,
}
struct Backend {
    observations: Arc<Mutex<Observations>>,
    store: ServiceViewStore<NatsStateStore>,
    availability: Value,
}
impl ServiceBackend<NatsStateStore, NatsStateStore, NatsMessageTransport> for Backend {
    fn connected(&self, connection: Connection) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let view = ActionAvailability::bind(connection.context().clone(), &self.store)?;
            view.wait_ready(Some(Duration::from_secs(15))).await?;
            let key = service_view_key(
                &connection.context().descriptor().service_id,
                FAMILY,
                &["current"],
            );
            view.create(
                &key,
                &ActionAvailabilityPayload::from_value(self.availability.clone())?,
            )
            .await?;
            let mut state = self.observations.lock().unwrap();
            state.connections.insert(
                connection
                    .context()
                    .agreement()
                    .contract()
                    .contract_id
                    .clone(),
                connection,
            );
            state.views.push(view);
            Ok(())
        })
    }
    fn handle(&self, incoming: Incoming) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            incoming.authorize()?;
            ActionMessage::from_envelope(&incoming.delivery.message)?;
            self.observations
                .lock()
                .unwrap()
                .received
                .push(serde_json::from_str(&incoming.delivery.message.to_text()?).unwrap());
            Ok(())
        })
    }
    fn shutdown(&self) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let views = std::mem::take(&mut self.observations.lock().unwrap().views);
            for view in views {
                view.close().await?;
            }
            Ok(())
        })
    }
}

fn declaration(root: &Path) -> Result<ValidatedServiceProtocol> {
    let mut registry = SchemaRegistry::default();
    let mut pending = vec![root.join("schemas")];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "json") {
                let schema: Value =
                    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
                if let Some(id) = schema.get("$id").and_then(Value::as_str) {
                    registry = registry.with_resource(id.to_owned(), schema)?;
                }
            }
        }
    }
    let protocol = ServiceProtocol::from_text(
        &std::fs::read_to_string(root.join("catalogs/action-runtime-protocol.v1.json")).unwrap(),
    )?;
    ValidatedServiceProtocol::new(protocol, &registry)
}
fn report(value: Value) {
    println!("{value}");
    std::io::stdout().flush().unwrap();
}
fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let config: Value = serde_json::from_str(&std::env::args().nth(1).expect("configuration"))?;
    let provider_role = config["role"] == "provider";
    let service_id = format!("action-runtime.{}", config["instance"].as_str().unwrap());
    let endpoint = if provider_role {
        format!("service:{service_id}")
    } else {
        format!("controller:{}", config["id"].as_str().unwrap())
    };
    let session_id = uuid::Uuid::new_v4().to_string();
    let protocol = declaration(Path::new(config["specRoot"].as_str().unwrap()))?;
    let runtime = DeckrRuntime::connect(config["url"].as_str().unwrap()).await?;
    let protocols = runtime.protocols().await?;
    let session = EndpointSession::with_id(EndpointAddress::parse(&endpoint)?, &session_id)?;
    let messages = runtime.messaging(session.clone())?;
    let store_name = &protocol.protocol().view_families[FAMILY].store_name;
    let store = runtime
        .service_views(
            store_name,
            StateStorePolicy::persistent("Action Runtime availability"),
        )
        .await?;
    let observations = Arc::new(Mutex::new(Observations::default()));
    let mut provider = None;
    let mut client = None;
    let mut directory = None;
    let mut reader = None;
    if provider_role {
        let availability = json!({"providerInstanceId":config["instance"], "providerId":config["providerId"], "serviceId":service_id, "serviceEndpoint":endpoint, "serviceSessionId":session_id, "entries":config["entries"]});
        ActionAvailabilityPayload::from_value(availability.clone())?;
        let advertisement = protocol.protocol().advertisement_payload(
            &service_id,
            &session_id,
            ServiceBackendStatus::Available,
        )?;
        let permissions = ServicePermissions::all(&ServiceDescriptor::from_payload(
            advertisement.clone(),
            protocol.protocol(),
        )?);
        let owner = ServiceProvider::start(ServiceProviderSpec {
            advertisements: runtime.beacon().clone(),
            concord: protocols.concord().clone(),
            messages: messages.clone(),
            protocol: protocol.clone(),
            advertisement,
            initial_consent: true,
            admission: Arc::new(move |_: ServiceConsumer| {
                let permissions = permissions.clone();
                async move { Ok(Some(permissions)) }
            }),
            backend: Arc::new(Backend {
                observations: observations.clone(),
                store: store.clone(),
                availability,
            }),
            options: ServiceProviderOptions {
                callback_timeout: Duration::from_secs(20),
                ..Default::default()
            },
        })?;
        owner.wait_ready(Some(Duration::from_secs(30))).await?;
        provider = Some(owner);
    } else {
        let discovery =
            ServiceDirectory::new(protocols.beacon().clone(), protocol.protocol().clone())?;
        discovery.try_start()?;
        let consumer = ServiceClient::start(ServiceClientSpec {
            directory: discovery.clone(),
            protocol: protocol.clone(),
            concord: protocols.concord().clone(),
            messages: messages.clone(),
            target: ServiceTarget::matching(ServiceQuery {
                service_id: Some(service_id.clone()),
                ..Default::default()
            }),
            policy: Arc::new(|descriptor: ServiceDescriptor| async move {
                Ok(Some(service_access(
                    &descriptor,
                    descriptor.supported_operations.iter().map(String::as_str),
                    BTreeMap::from([(FAMILY.into(), vec![])]),
                )?))
            }),
            options: ServiceClientOptions {
                negotiation_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        })?;
        let mut stream = consumer.watch_messages()?;
        consumer.wait_ready(Some(Duration::from_secs(30))).await?;
        let state = observations.clone();
        reader = Some(tokio::spawn(async move {
            while let Some(incoming) = stream.next().await {
                match incoming.and_then(|incoming| {
                    incoming.authorize()?;
                    ActionMessage::from_envelope(&incoming.delivery.message)?;
                    Ok(incoming.delivery.message)
                }) {
                    Ok(message) => state
                        .lock()
                        .unwrap()
                        .received
                        .push(serde_json::from_str(&message.to_text().unwrap()).unwrap()),
                    Err(error) => state.lock().unwrap().errors.push(error.to_string()),
                }
            }
        }));
        client = Some(consumer);
        directory = Some(discovery);
    }
    report(json!({"ready":true,"sessionId":session_id,"endpoint":endpoint}));
    let (send, mut input) = tokio::sync::mpsc::unbounded_channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            if send.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    while let Some(line) = input.recv().await {
        let command: Value = serde_json::from_str(&line)?;
        if command["op"] == "stop" {
            report(json!({"ok":true}));
            break;
        }
        let result: Result<Value> = async {
            let connection = if let Some(client) = &client { Some(client.wait_ready(Some(Duration::from_secs(15))).await?) }
                else { observations.lock().unwrap().connections.values().find(|c| c.authorize().is_ok()).cloned() };
            match command["op"].as_str().unwrap() {
                "status" => {
                    let state = observations.lock().unwrap();
                    let contract = connection.as_ref().map(|c| { let p = c.context().agreement().contract(); json!({"contractId":p.contract_id,"generation":p.generation}) });
                    let mut errors = state.errors.clone();
                    if let Some(owner) = &provider {
                        if let Some(failure) = owner.state().failure { errors.push(format!("{failure:?}")); }
                    }
                    Ok(json!({"contract":contract,"received":state.received,"errors":errors}))
                },
                "availability" => {
                    let connection = connection.ok_or_else(|| invalid("no current connection"))?;
                    let view = ActionAvailability::bind(connection.context().clone(), &store)?;
                    view.wait_ready(Some(Duration::from_secs(15))).await?;
                    let entry = view.get(&service_view_key(&service_id, FAMILY, &["current"])).await?;
                    let result = entry.map(|entry| entry.record.availability.to_value()).transpose()?.unwrap_or(json!({}));
                    view.close().await?;
                    Ok(result)
                },
                "send" => {
                    let connection = if let Some(id) = command["contractId"].as_str() { observations.lock().unwrap().connections.get(id).cloned() } else { connection }.ok_or_else(|| invalid("no connection"))?;
                    let name = command["name"].as_str().unwrap();
                    let definition = &protocol.protocol().messages[name];
                    let mut body = json!({"serviceNamespace":protocol.protocol().namespace,"name":name,"intent":definition.intent,"exchangePattern":"one_way"});
                    body[if definition.intent == ServiceIntent::Event { "event" } else { "params" }] = command["payload"].clone();
                    let action = ActionMessage::from_body(&ServiceMessageBody::from_value(body)?)?;
                    let mut message = connection.message(action.to_body()?)?;
                    if name == "binding_render_result" { message.causation_id = command["payload"]["requestMessageId"].as_str().map(str::to_owned); }
                    ActionMessage::from_envelope(&message)?;
                    connection.send(message.clone()).await?;
                    Ok(json!({"messageId":message.message_id,"message":serde_json::from_str::<Value>(&message.to_text()?).unwrap()}))
                },
                "authorize" => {
                    let message = DeckrMessage::from_text(&command["message"].to_string())?;
                    connection.ok_or_else(|| invalid("no connection"))?.authorize_incoming(&message)?;
                    Ok(json!({"authorized":true}))
                },
                op => Err(invalid(format!("unknown operation {op}"))),
            }
        }.await;
        report(result.unwrap_or_else(|error| json!({"error":error.to_string()})));
    }
    if let Some(client) = client {
        client.close().await?;
    }
    if let Some(provider) = provider {
        provider.close().await?;
    }
    if let Some(reader) = reader {
        reader.abort();
    }
    if let Some(directory) = directory {
        directory.close_and_wait().await;
    }
    session.close();
    messages.close().await?;
    protocols.close().await;
    runtime.nats().close_sources().await;
    Ok(())
}
