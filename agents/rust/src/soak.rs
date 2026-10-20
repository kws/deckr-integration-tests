use super::*;
type Client = ServiceClient<NatsStateStore, NatsStateStore, NatsMessageTransport>;
type Connection = ServiceConnection<NatsStateStore, NatsStateStore, NatsMessageTransport>;

pub fn enabled() -> bool {
    env::var("MODE").as_deref() == Ok("soak")
}

async fn epoch_value() -> Result<u64, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match fs::read_to_string("/control/epoch") {
            Ok(value) => return Ok(value.trim().parse()?),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound && Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

pub async fn consume(
    client: &Client,
    store: &ServiceViewStore<NatsStateStore>,
    settings: &Settings,
    cases: &[Value],
) -> Result<(), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs_f64(
        env::var("OPERATION_TIMEOUT")
            .unwrap_or_else(|_| "10".into())
            .parse()?,
    );
    let mut previous: Option<(Connection, View)> = None;
    let mut sequence = 0_u64;
    while !Path::new("/control/stop").exists() {
        let seq = sequence;
        sequence += 1;
        let epoch = epoch_value().await?;
        let case_index = seq as usize % cases.len();
        let operation_id = format!("{}-{}-{seq}", settings.id, settings.session);
        let started = Instant::now();
        let attempt: Result<(), Box<dyn std::error::Error>> = async {
            let connection = client.wait_ready(Some(timeout)).await?;
            let context = connection.context();
            let held = context.agreement().contract();
            let contract = json!({"contractId":held.contract_id,"generation":held.generation});
            let descriptor = context.descriptor();
            let provider_session = descriptor.session_id.clone();
            let previous_contract = previous.as_ref().map(|(old, _)| { let held = old.context().agreement().contract(); json!({"contractId":held.contract_id,"generation":held.generation}) });
            let mut rejected = None;
            let same = previous_contract.as_ref() == Some(&contract);
            if previous.is_some() && !same {
                rejected = Some(previous.as_ref().unwrap().0.authorize().is_err());
                assert_eq!(rejected, Some(true), "retired agreement regained authority");
            }
            let view = if same { previous.as_ref().unwrap().1.clone() } else { connection.view(store, "results", None)? };
            view.wait_ready(Some(timeout)).await?;
            let body = ServiceMessageBody {
                service_namespace: descriptor.namespace.clone(), name: "echo".into(), intent: ServiceIntent::Query,
                exchange_pattern: ExchangePattern::RequestReply, params: json!({"operationId":operation_id,"payload":cases[case_index]}).as_object().unwrap().clone(),
                event: None, result: None, error: None, status: None,
            };
            let reply = connection.request(connection.message(body)?, timeout, None).await?.service_body()?;
            if reply.status != Some(ServiceMessageStatus::Ok) { return Err("request unavailable".into()); }
            let reply = json!(reply.result.unwrap());
            let expected = json!({"operationId":operation_id,"payload":cases[case_index],"providerLanguage":settings.target,"providerSession":provider_session});
            assert_eq!(reply, expected);
            let key = format!("{}latest", descriptor.views["results"].key_prefix);
            let entry = tokio::time::timeout(timeout, async {
                loop {
                    if let Some(entry) = view.get(&key).await? {
                        if entry.record.payload.get("operationId") == Some(&json!(operation_id)) { break deckr::Result::Ok(entry); }
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }).await??;
            let actual = json!(entry.record.payload);
            assert_eq!(actual, expected);
            assert_eq!(held.generation, 1);
            settings.event("success", json!({"seq":seq,"epoch":epoch,"caseIndex":case_index,"operationId":operation_id,"contract":contract,"previousContract":previous_contract,"oldAuthorityRejected":rejected,"providerSession":provider_session,"reply":reply,"view":actual,"latencyMs":started.elapsed().as_secs_f64()*1000.0}))?;
            if !same { if let Some((_, old_view)) = previous.take() { old_view.close().await?; } }
            previous = Some((connection, view));
            Ok(())
        }.await;
        if let Err(error) = attempt {
            settings.event("unavailable", json!({"seq":seq,"epoch":epoch,"operationId":operation_id,"error":error.to_string()}))?;
        }
        tokio::time::sleep(Duration::from_secs_f64(
            env::var("OPERATION_INTERVAL")
                .unwrap_or_else(|_| "1".into())
                .parse()?,
        ))
        .await;
    }
    if let Some((_, view)) = previous {
        view.close().await?;
    }
    Ok(())
}
