//! The start of a pull against the Pub/Sub emulator (testcontainers): a
//! missing subscription fails the connect, checked or not, and a message
//! that comes while the start watches the stream is received first.
//!
//! Gated behind the `integration-tests` feature:
//!   cargo test -p mcpg-plugin-watch-pubsub --features integration-tests --lib

use gcloud_googleapis::pubsub::v1::PubsubMessage;
use gcloud_pubsub::subscription::SubscriptionConfig;
use mcpg_testcontainer_support::{
    NoEgress, PUBSUB_EMULATOR_PORT, pubsub_emulator, start_with_retry_async,
};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

use super::*;

/// An emulator with topic `orders` and its subscription `orders-sub`, and
/// its `host:port`.
async fn emulator() -> (ContainerAsync<GenericImage>, String) {
    let container =
        start_with_retry_async("pubsub", || pubsub_emulator().no_egress().start()).await;
    let port = container
        .get_host_port_ipv4(PUBSUB_EMULATOR_PORT)
        .await
        .expect("port");
    let host = format!("127.0.0.1:{port}");
    let admin = client(&host).await;
    admin
        .create_topic("orders", None, None)
        .await
        .expect("create topic orders");
    admin
        .create_subscription("orders-sub", "orders", SubscriptionConfig::default(), None)
        .await
        .expect("create subscription orders-sub");
    (container, host)
}

/// A client of the emulator, configured as the watch configures its own.
async fn client(host: &str) -> Client {
    let config = client_config(Some("test-project".into()), None, Some(host))
        .await
        .expect("an emulator configuration");
    Client::new(config).await.expect("a client")
}

/// A new StreamingPull on subscription `id`, with the watch's settings, and
/// its full name.
async fn subscribe(client: &Client, id: &str) -> (String, MessageStream) {
    let subscription = client.subscription(id);
    let stream = subscription
        .subscribe(Some(subscribe_config(60, 16)))
        .await
        .expect("subscribe opens the stream");
    (subscription.fully_qualified_name().to_owned(), stream)
}

fn refused(name: &str) -> Option<Unconfirmed> {
    Some(Unconfirmed {
        name: name.to_owned(),
        refusal: "User not authorized to perform this action.".to_owned(),
    })
}

/// The watch's own connect to subscription `id`. It runs on a blocking
/// thread, so the source drops its runtime outside the test's.
async fn connect(host: &str, id: &str) -> Result<(), String> {
    let (host, id) = (host.to_owned(), id.to_owned());
    tokio::task::spawn_blocking(move || {
        let source = PubsubSubscription {
            subscription: id,
            project: Some("test-project".into()),
            credentials_json: None,
            emulator_host: Some(host),
            deadline_secs: 60,
            max_outstanding: 16,
            max_body_bytes: MAX_BODY_BYTES,
            rt: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("a runtime"),
            pull: Mutex::new(None),
            held: Mutex::new(HashMap::new()),
            next_receipt: AtomicU64::new(0),
        };
        source.connect()
    })
    .await
    .expect("the connect thread")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_subscription_fails_the_connect_checked_or_not() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (_container, host) = emulator().await;

    assert_eq!(
        connect(&host, "no-such-sub").await,
        Err(
            "Pub/Sub subscription projects/test-project/subscriptions/no-such-sub does not exist"
                .into()
        ),
        "the emulator answers the check"
    );
    assert_eq!(connect(&host, "orders-sub").await, Ok(()));

    let client = client(&host).await;
    let (name, stream) = subscribe(&client, "no-such-sub").await;
    let error = Pull::start(stream, refused(&name))
        .await
        .err()
        .expect("the start fails");
    assert!(
        error.contains(&format!("subscription {name} may not exist"))
            && error.ends_with(
                "the subscription check was refused: User not authorized to perform this action."
            ),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_message_that_comes_while_the_start_waits_is_received_first() {
    let _ = tracing_subscriber::fmt().with_test_writer().try_init();
    let (_container, host) = emulator().await;
    let client = client(&host).await;
    let mut publisher = client.topic("orders").new_publisher(None);
    let published = publisher
        .publish(PubsubMessage {
            data: b"first".to_vec(),
            ..PubsubMessage::default()
        })
        .await
        .get()
        .await
        .expect("published");
    publisher.shutdown().await;

    let (name, stream) = subscribe(&client, "orders-sub").await;
    let Ok(mut pull) = Pull::start(stream, refused(&name)).await else {
        panic!("the start of an existing subscription fails");
    };
    assert!(
        pull.first.is_some() && pull.unconfirmed.is_none(),
        "the start took the message, which confirms the subscription"
    );
    let received = pull
        .receive(10, Duration::from_secs(1))
        .await
        .expect("a receive");
    let ids: Vec<&str> = received
        .iter()
        .map(|m| m.message.message_id.as_str())
        .collect();
    assert_eq!(ids, [published.as_str()]);
    for message in received {
        message.ack().await.expect("acknowledged");
    }
    pull.dispose().await;
}
