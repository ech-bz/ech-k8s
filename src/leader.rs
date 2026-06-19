use kube::Client;
use kube_lease_manager::LeaseManagerBuilder;
use serde::Deserialize;
use std::{fmt::Debug, future::Future};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Clone, Debug, Deserialize)]
pub struct LeaderSettings {
    pub lease_namespace: String,
    pub lease_name: String,
    pub holder_id: String,
    pub lease_duration_seconds: u64,
    pub lease_grace_seconds: u64,
}

pub(crate) async fn run<F, Fut, E>(
    client: kube::Client,
    settings: LeaderSettings,
    shutdown: CancellationToken,
    reconciler: F,
) -> Result<(), E>
where
    F: Fn(Client, CancellationToken) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<(), E>> + Send + 'static,
    E: From<kube_lease_manager::LeaseManagerError> + Debug + Send + Sync + 'static,
{
    let manager = LeaseManagerBuilder::new(client.clone(), settings.lease_name.clone())
        .with_namespace(settings.lease_namespace.clone())
        .with_identity(settings.holder_id.clone())
        .with_duration(settings.lease_duration_seconds)
        .with_grace(settings.lease_grace_seconds)
        .build()
        .await?;

    let (mut state, lease_task) = manager.watch().await;
    let mut current: Option<(CancellationToken, JoinHandle<Result<(), E>>)> = None;

    loop {
        let reconciler_done = async {
            match current.as_mut() {
                Some((_, handle)) => (&mut *handle).await,
                None => std::future::pending().await,
            }
        };

        tokio::select! {
            _ = shutdown.cancelled() => break,
            res = reconciler_done => {
                warn!(result = ?res, "reconciler task exited unexpectedly, releasing lease");
                shutdown.cancel();
                break;
            }
            changed = state.changed() => {
                if changed.is_err() {
                    warn!("lease manager watch channel closed");
                    shutdown.cancel();
                    break;
                }
                let is_leader = *state.borrow_and_update();
                if is_leader && current.is_none() {
                    info!(lease = %settings.lease_name, "leader lease acquired");
                    let token = shutdown.child_token();
                    let task_token = token.clone();
                    let task_client = client.clone();
                    let task_reconciler = reconciler.clone();
                    let handle = tokio::spawn(async move {
                        task_reconciler(task_client, task_token).await
                    });
                    current = Some((token, handle));
                } else if !is_leader && current.is_some() {
                    info!("leader lease lost");
                    if let Some((token, handle)) = current.take() {
                        token.cancel();
                        if let Err(err) = handle.await {
                            warn!(error = %err, "reconciler join failed");
                        }
                    }
                }
            }
        }
    }

    if let Some((token, handle)) = current.take() {
        token.cancel();
        if let Err(err) = handle.await {
            warn!(error = %err, "reconciler join failed during shutdown");
        }
    }
    drop(state);
    let _ = lease_task.await;
    info!("leader runner exiting");
    Ok(())
}
