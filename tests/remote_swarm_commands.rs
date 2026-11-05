//! Regression test for a lost wakeup in the swarm behaviour's command processing.
//!
//! `Behaviour::poll` took a single command from its command channel per poll and only woke
//! itself again if handling that command requested it. Commands which do not need a re-poll
//! (`LookupLocal`, `Unregister`) therefore left the channel without a registered waker: the
//! next command (and every one after it) was only processed once an unrelated swarm event, such
//! as a Kademlia or mDNS timer, happened to poll the swarm. On an idle swarm that can take
//! minutes.
//!
//! The swarm's startup timers (e.g. Kademlia's automatic bootstrap) mask the bug, so the test
//! first waits for the swarm to go idle. mDNS traffic from other kameo peers on the network can
//! mask it as well, making this test pass spuriously in such an environment.
#![cfg(feature = "remote")]

use std::time::Duration;

use kameo::prelude::*;

#[derive(Actor, RemoteActor)]
struct TestActor;

/// Lookups of missing actors should return `None` and not block indefinitely
#[tokio::test]
async fn multiple_lookups_are_processed() {
    remote::bootstrap_on("/ip4/127.0.0.1/tcp/0").expect("swarm should bootstrap");
    // wait for bootstrapping to complete
    // we need to wait here because kadmelia's timers to pass
    tokio::time::sleep(Duration::from_secs(2)).await;

    let lookup_1 = ActorRef::<TestActor>::lookup("missing")
        .await
        .expect("lookup should succeed");
    assert!(lookup_1.is_none());

    let lookup_2_fut = RemoteActorRef::<TestActor>::lookup("missing");
    let lookup_2 = tokio::time::timeout(Duration::from_secs(2), lookup_2_fut)
        .await
        .expect("lookup should not time out")
        .expect("lookup should succeed");
    assert!(lookup_2.is_none());

    // same test for unregister command
    remote::unregister("missing")
        .await
        .expect("unregister should succeed");

    let unregister_fut = remote::unregister("missing");
    tokio::time::timeout(Duration::from_secs(2), unregister_fut)
        .await
        .expect("unregister should not time out")
        .expect("unregister should succeed");
}
