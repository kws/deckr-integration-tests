use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deckr::endpoint::EndpointAddress;
use deckr::nats::{DeckrRuntime, NatsMessageTransport, NatsStateStore};
use deckr::schema::SchemaRegistry;
use deckr::services::*;
use deckr::session::EndpointSession;
use deckr::state::StateStorePolicy;
use serde_json::{json, Value};
mod soak;

type Incoming = ServiceIncoming<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type View = ProtectedServiceView<NatsStateStore, NatsStateStore, NatsStateStore>;

fn append_event(path: &Path, record: &Value) -> std::io::Result<()> {
    // Format before opening the append transaction. Formatting directly into
    // the file writes each JSON fragment separately: a crash can leave half an
    // event that the restarted process then joins to its first event.
    let mut bytes = serde_json::to_vec(record)?;
    bytes.push(b'\n');
    static APPEND_LOCK: Mutex<()> = Mutex::new(());
    let _guard = APPEND_LOCK.lock().unwrap();
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(&bytes)
}

#[derive(Clone)]
struct Settings {
    role: String,
    target: String,
    id: String,
    session: String,
    run_id: String,
}
impl Settings {
    fn event(&self, kind: &str, data: Value) -> std::io::Result<()> {
        let mut record = json!({"runId":self.run_id,"id":self.id,"language":"rust","role":self.role,"target":self.target,"sessionId":self.session,"kind":kind,"atMs":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis()});
        record
            .as_object_mut()
            .unwrap()
            .extend(data.as_object().unwrap().clone());
        append_event(
            Path::new(&format!("/artifacts/{}.events.jsonl", self.id)),
            &record,
        )
    }
    fn report(&self, kind: &str, data: Value) -> std::io::Result<()> {
        let mut record = json!({"runId":self.run_id, "id":self.id, "language":"rust", "role":self.role, "target":self.target, "sessionId":self.session});
        record
            .as_object_mut()
            .unwrap()
            .extend(data.as_object().unwrap().clone());
        let path = format!("/artifacts/{}.{}.json", self.id, kind);
        fs::write(format!("{path}.tmp"), serde_json::to_vec(&record)?)?;
        fs::rename(format!("{path}.tmp"), path)?;
        println!("{}", json!({"event":kind,"data":record}));
        if soak::enabled() {
            self.event(kind, data)?;
        }
        Ok(())
    }
}

struct Backend {
    store: ServiceViewStore<NatsStateStore>,
    retained: Mutex<BTreeMap<String, View>>,
    session: String,
    settings: Settings,
}
impl ServiceBackend<NatsStateStore, NatsStateStore, NatsMessageTransport> for Backend {
    fn handle(&self, incoming: Incoming) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let body = incoming.delivery.message.service_body()?;
            let mut result = body.params.clone();
            result.insert("providerLanguage".into(), json!("rust"));
            result.insert("providerSession".into(), json!(self.session));
            let context = incoming.connection.context();
            let key = format!(
                "{}{}",
                context.descriptor().views["results"].key_prefix,
                if soak::enabled() {
                    "latest"
                } else {
                    body.params["operationId"].as_str().unwrap()
                }
            );
            let pointer = context.agreement().contract().contract_id.clone();
            let existing = self.retained.lock().unwrap().get(&pointer).cloned();
            let writer = match existing {
                Some(writer) => writer,
                None => incoming.connection.view(&self.store, "results", None)?,
            };
            writer.wait_ready(Some(Duration::from_secs(15))).await?;
            if soak::enabled() {
                self.settings
                    .event(
                        "execution",
                        json!({"operationId":body.params["operationId"]}),
                    )
                    .map_err(|error| deckr::Error::Invalid(error.to_string()))?;
                if let Some(entry) = writer.get(&key).await? {
                    writer.update(&key, result.clone(), entry.revision).await?;
                } else {
                    writer.create(&key, result.clone()).await?;
                }
            } else {
                writer.create(&key, result.clone()).await?;
            }
            self.retained.lock().unwrap().insert(pointer, writer);
            incoming.authorize()?;
            let mut response = body;
            response.params.clear();
            response.status = Some(ServiceMessageStatus::Ok);
            response.result = Some(result);
            incoming.reply(response).await
        })
    }
    fn disconnected(
        &self,
        connection: ServiceConnection<NatsStateStore, NatsStateStore, NatsMessageTransport>,
    ) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let pointer = connection
                .context()
                .agreement()
                .contract()
                .contract_id
                .clone();
            let writer = self.retained.lock().unwrap().remove(&pointer);
            if let Some(writer) = writer {
                writer.close().await?;
            }
            Ok(())
        })
    }
    fn shutdown(&self) -> ServiceBackendFuture<'_> {
        Box::pin(async move {
            let retained = std::mem::take(&mut *self.retained.lock().unwrap());
            for view in retained.into_values() {
                view.close().await?;
            }
            Ok(())
        })
    }
}

async fn run(settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    if soak::enabled() {
        settings.event("boot", json!({}))?;
    }
    let directory = env::var("PROTOCOL_DIR").unwrap_or_else(|_| "/protocol".into());
    let declaration =
        ServiceProtocol::from_text(&fs::read_to_string(format!("{directory}/service.json"))?)?;
    let cases: Vec<Value> =
        serde_json::from_str(&fs::read_to_string(format!("{directory}/cases.json"))?)?;
    let store_name = declaration.view_families["results"].store_name.clone();
    let protocol = ValidatedServiceProtocol::new(declaration.clone(), &SchemaRegistry::default())?;
    let runtime = DeckrRuntime::connect(env::var("NATS_URL")?).await?;
    eprintln!("{}: transport connected", settings.id);
    let protocols = runtime.protocols().await?;
    eprintln!("{}: protocol sources ready", settings.id);
    let endpoint = if settings.role == "provider" {
        "service:interop-rust".to_owned()
    } else {
        format!("controller:{}", settings.id)
    };
    let session = EndpointSession::with_id(EndpointAddress::parse(endpoint)?, &settings.session)?;
    println!("{}", json!({"event":"endpoint-session", "endpoint":session.endpoint(), "sessionId":session.id()}));
    let messages = runtime.messaging(session.clone())?;
    let store = runtime
        .service_views(
            &store_name,
            StateStorePolicy::persistent("integration results"),
        )
        .await?;
    eprintln!("{}: service view store ready", settings.id);

    if settings.role == "provider" {
        let backend = Arc::new(Backend {
            store: store.clone(),
            retained: Mutex::default(),
            session: settings.session.clone(),
            settings: settings.clone(),
        });
        let permissions = ServicePermissions {
            operations: ["echo".into()].into(),
            views: BTreeMap::from([("results".into(), vec![])]),
        };
        let provider = ServiceProvider::start(ServiceProviderSpec {
            advertisements: runtime.beacon().clone(),
            concord: protocols.concord().clone(),
            messages: messages.clone(),
            advertisement: declaration.advertisement_payload(
                "interop-rust",
                &settings.session,
                ServiceBackendStatus::Available,
            )?,
            protocol,
            initial_consent: true,
            admission: Arc::new(move |_: ServiceConsumer| {
                let permissions = permissions.clone();
                async move { Ok(Some(permissions)) }
            }),
            backend,
            options: ServiceProviderOptions {
                callback_timeout: Duration::from_secs(20),
                ..Default::default()
            },
        })?;
        // A restarted provider can wait for the previous session's 120-second
        // token to expire. The runner owns the startup/recovery deadline.
        let ready = provider.wait_ready(None);
        tokio::pin!(ready);
        loop {
            tokio::select! {
                result = &mut ready => { result?; break; }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    eprintln!("{}: provider startup {:?}", settings.id, provider.state());
                }
            }
        }
        settings.report("ready", json!({"serviceId":"interop-rust"}))?;
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
        provider.close().await?;
    } else {
        assert_eq!(settings.role, "client");
        let directory = ServiceDirectory::new(protocols.beacon().clone(), declaration)?;
        directory.try_start()?;
        let client = ServiceClient::start(ServiceClientSpec {
            directory: directory.clone(),
            protocol,
            concord: protocols.concord().clone(),
            messages: messages.clone(),
            target: ServiceTarget::matching(ServiceQuery {
                service_id: Some(format!("interop-{}", settings.target)),
                ..Default::default()
            }),
            policy: Arc::new(|descriptor: ServiceDescriptor| async move {
                Ok(Some(service_access(
                    &descriptor,
                    ["echo"],
                    BTreeMap::from([("results".into(), vec![])]),
                )?))
            }),
            options: ServiceClientOptions {
                negotiation_timeout: Duration::from_secs(30),
                ..Default::default()
            },
        })?;
        let connection = client.wait_ready(Some(Duration::from_secs(60))).await?;
        let context = connection.context();
        let held = context.agreement().contract();
        let contract = json!({"contractId":held.contract_id, "generation":held.generation});
        let descriptor = context.descriptor();
        assert_eq!(
            descriptor.service_id,
            format!("interop-{}", settings.target)
        );
        assert_eq!(held.generation, 1);
        settings.report(
            "ready",
            json!({"contract":contract,"providerSession":descriptor.session_id}),
        )?;
        let deadline = Instant::now() + Duration::from_secs(90);
        while !Path::new("/control/start").exists() {
            if Instant::now() > deadline {
                return Err("start barrier timed out".into());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if soak::enabled() {
            soak::consume(&client, &store, settings, &cases).await?;
            settings.report("result", json!({"status":"pass"}))?;
        } else {
            let view = connection.view(&store, "results", None)?;
            view.wait_ready(Some(Duration::from_secs(15))).await?;
            let mut observations = Vec::new();
            let rounds: usize = env::var("ROUNDS").unwrap_or_else(|_| "2".into()).parse()?;
            for round in 0..rounds {
                for (case_index, payload) in cases.iter().enumerate() {
                    let operation_id = format!("{}-{round}-{case_index}", settings.id);
                    let expected = json!({"operationId":operation_id,"payload":payload,"providerLanguage":settings.target,"providerSession":descriptor.session_id});
                    let body = ServiceMessageBody {
                        service_namespace: "dev.deckr.integration".into(),
                        name: "echo".into(),
                        intent: ServiceIntent::Query,
                        exchange_pattern: ExchangePattern::RequestReply,
                        params: json!({"operationId":operation_id,"payload":payload})
                            .as_object()
                            .unwrap()
                            .clone(),
                        event: None,
                        result: None,
                        error: None,
                        status: None,
                    };
                    let message = connection.message(body)?;
                    let response = connection
                        .request(message, Duration::from_secs(15), None)
                        .await?;
                    let reply = response.service_body()?;
                    assert_eq!(reply.status, Some(ServiceMessageStatus::Ok));
                    let reply = json!(reply.result.unwrap());
                    assert_eq!(reply, expected);
                    let key = format!("{}{}", descriptor.views["results"].key_prefix, operation_id);
                    let entry = tokio::time::timeout(Duration::from_secs(15), async {
                        loop {
                            if let Some(entry) = view.get(&key).await? {
                                break deckr::Result::Ok(entry);
                            }
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                    })
                    .await??;
                    let actual = json!(entry.record.payload);
                    assert_eq!(actual, expected);
                    observations.push(json!({"round":round,"caseIndex":case_index,"operationId":operation_id,"reply":reply,"view":actual}));
                }
            }
            settings.report("result", json!({"status":"pass","contract":contract,"providerSession":descriptor.session_id,"observations":observations}))?;
            view.close().await?;
        }
        client.close().await?;
        directory.close_and_wait().await;
    }
    session.close();
    messages.close().await?;
    protocols.close().await;
    runtime.nats().close_sources().await;
    Ok(())
}

#[tokio::main]
async fn main() {
    let role = env::var("ROLE").expect("ROLE is required");
    let target = env::var("TARGET_LANGUAGE").unwrap_or_else(|_| "rust".into());
    let id = env::var("PARTICIPANT_ID").unwrap_or_else(|_| {
        format!(
            "{role}-rust-{target}-{}",
            env::var("HOSTNAME").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string())
        )
    });
    let settings = Settings {
        role,
        target,
        id,
        session: uuid::Uuid::new_v4().to_string(),
        run_id: env::var("RUN_ID").expect("RUN_ID is required"),
    };
    if let Err(error) = run(&settings).await {
        let _ = settings.report("result", json!({"status":"fail","error":error.to_string()}));
        eprintln!("{error:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_events_remain_complete_json_records() {
        let path = env::temp_dir().join(format!("deckr-events-{}.jsonl", uuid::Uuid::new_v4()));
        std::thread::scope(|scope| {
            for writer in 0..8 {
                let path = &path;
                scope.spawn(move || {
                    for sequence in 0..100 {
                        append_event(
                            path,
                            &json!({"writer":writer,"sequence":sequence,"payload":"x".repeat(4096)}),
                        )
                        .unwrap();
                    }
                });
            }
        });
        let contents = fs::read_to_string(&path).unwrap();
        fs::remove_file(path).unwrap();
        assert!(contents.ends_with('\n'));
        let records: std::collections::BTreeSet<_> = contents
            .lines()
            .map(|line| {
                let event: Value = serde_json::from_str(line).unwrap();
                assert_eq!(event["payload"].as_str().unwrap().len(), 4096);
                (
                    event["writer"].as_u64().unwrap(),
                    event["sequence"].as_u64().unwrap(),
                )
            })
            .collect();
        assert_eq!(records.len(), 800);
    }
}
