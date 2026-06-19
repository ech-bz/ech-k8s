use crate::{
    CrMeta, K8sClient, LeaderSettings, ResourcesExt, leader,
    workflow::{Scheduler, Workflow, WorkflowError},
};
use axum::{Router, routing::get};
use futures::StreamExt;
use kube::{
    Resource,
    api::Api,
    core::NamespaceResourceScope,
    runtime::{
        controller::{Action, Controller},
        finalizer::{Error as FinalizerError, Event as FinalizerEvent, finalizer},
        watcher,
    },
};
use serde::{Serialize, de::DeserializeOwned};
use std::{error::Error as StdError, fmt::Debug, hash::Hash, sync::Arc, time::Duration};
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[derive(Clone)]
pub struct RuntimeSettings {
    pub health_port: u16,
    pub reconcile_interval: Duration,
    pub error_backoff: Duration,
}

pub struct OperatorSpec<W> {
    pub runtime: RuntimeSettings,
    pub leader: LeaderSettings,
    pub field_manager: &'static str,
    pub workflow: W,
}

pub struct Operator;

impl Operator {
    pub async fn run<W>(spec: OperatorSpec<W>) -> Result<(), W::Error>
    where
        W: Workflow,
        W::Crd: Resource<Scope = NamespaceResourceScope>
            + Clone
            + Debug
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + 'static,
        <W::Crd as Resource>::DynamicType: Default + Eq + Hash + Clone + Debug + Unpin,
        W::Error: StdError
            + From<String>
            + From<kube::Error>
            + From<kube_lease_manager::LeaseManagerError>
            + Send
            + Sync
            + 'static,
    {
        let field_manager = spec.field_manager;
        let leader = spec.leader.clone();
        let health_port = spec.runtime.health_port;
        let spec = Arc::new(spec);
        let shutdown = CancellationToken::new();
        let health_task = spawn_health_server(health_port, shutdown.clone());
        spawn_signal_handler(shutdown.clone());

        let k8s = K8sClient::try_new(field_manager)
            .await
            .map_err(W::Error::from)?;

        let result = leader::run(
            k8s.client().clone(),
            leader,
            shutdown.clone(),
            move |_client, shutdown| {
                let spec = Arc::clone(&spec);
                let k8s = k8s.clone();
                async move { run_controller(spec, k8s, shutdown).await }
            },
        )
        .await;

        shutdown.cancel();
        if let Some(handle) = health_task {
            if let Err(err) = handle.await {
                warn!(error = %err, "health server join failed");
            }
        }

        result
    }
}

async fn run_controller<W>(
    spec: Arc<OperatorSpec<W>>,
    k8s: K8sClient,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<(), W::Error>
where
    W: Workflow,
    W::Crd: Resource<Scope = NamespaceResourceScope>
        + Clone
        + Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync
        + 'static,
    <W::Crd as Resource>::DynamicType: Default + Eq + Hash + Clone + Debug + Unpin,
    W::Error: StdError
        + From<String>
        + From<kube::Error>
        + From<kube_lease_manager::LeaseManagerError>
        + Send
        + Sync
        + 'static,
{
    let error_backoff = spec.runtime.error_backoff;

    Controller::new(k8s.all::<W::Crd>().into_inner(), watcher::Config::default())
        .graceful_shutdown_on(shutdown.clone().cancelled_owned())
        .run(
            move |cr, ctl| {
                let spec = Arc::clone(&spec);
                let ctl = Arc::clone(&ctl);
                async move {
                    let ns = cr.cr_ns().map_err(|e| W::Error::from(e.to_string()))?;
                    let name = cr.cr_name().map_err(|e| W::Error::from(e.to_string()))?;
                    let api: Api<W::Crd> = Api::namespaced((*ctl).clone(), &ns);
                    let reconciler_name = format!("ech.bz/{}", spec.field_manager);
                    finalizer(&api, &reconciler_name, cr, move |event| {
                        let spec = Arc::clone(&spec);
                        let ctl = Arc::clone(&ctl);
                        let client = K8sClient {
                            field_manager: spec.field_manager.to_string(),
                            client: (*ctl).clone(),
                        };
                        let ns = ns.clone();
                        let name = name.clone();
                        async move {
                            match event {
                                FinalizerEvent::Apply(cr) => {
                                    let graph = spec
                                        .workflow
                                        .build_graph(&cr)
                                        .map_err(|err| W::Error::from(err.to_string()))?;
                                    let scheduler = Scheduler::new(graph)
                                        .map_err(|err| W::Error::from(err.to_string()))?;
                                    let status = scheduler
                                        .run(&client, &cr, cr.meta().generation)
                                        .await
                                        .map_err(map_workflow_error::<W::Error>)?;
                                    client
                                        .namespaced::<W::Crd>(&ns)
                                        .patch_status(&name, &status)
                                        .await
                                        .map_err(W::Error::from)?;
                                    if status.pending.is_empty() {
                                        Ok(Action::await_change())
                                    } else {
                                        Ok(Action::requeue(spec.runtime.reconcile_interval))
                                    }
                                }
                                FinalizerEvent::Cleanup(cr) => {
                                    let graph = spec
                                        .workflow
                                        .build_graph(&cr)
                                        .map_err(|err| W::Error::from(err.to_string()))?;
                                    let scheduler = Scheduler::new(graph)
                                        .map_err(|err| W::Error::from(err.to_string()))?;
                                    scheduler
                                        .cleanup(&client, &cr)
                                        .await
                                        .map_err(map_workflow_error::<W::Error>)?;
                                    Ok(Action::await_change())
                                }
                            }
                        }
                    })
                    .await
                    .map_err(map_finalizer_error::<W::Error>)
                }
            },
            move |_cr, _err, _ctl| {
                warn!(backoff = ?error_backoff, "operator reconcile failed");
                Action::requeue(error_backoff)
            },
            Arc::new(k8s.client().clone()),
        )
        .for_each(|res| async move {
            if let Err(err) = res {
                warn!(error = %err, "operator stream error");
            }
        })
        .await;

    if shutdown.is_cancelled() {
        Ok(())
    } else {
        Err(W::Error::from(
            "controller stream terminated unexpectedly".to_string(),
        ))
    }
}

fn spawn_signal_handler(token: CancellationToken) {
    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(err) => {
                warn!(error = %err, "failed to install SIGTERM handler");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => warn!("received SIGINT"),
            _ = term.recv() => warn!("received SIGTERM"),
        }
        token.cancel();
    });
}

fn spawn_health_server(port: u16, shutdown: CancellationToken) -> Option<JoinHandle<()>> {
    if port == 0 {
        return None;
    }

    Some(tokio::spawn(async move {
        let app = Router::new()
            .route("/healthz", get(ok))
            .route("/readyz", get(ok));

        let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
            Ok(listener) => listener,
            Err(err) => {
                warn!(error = %err, port, "health server bind failed");
                shutdown.cancel();
                return;
            }
        };

        warn!(port, "health server listening");
        let shutdown_for_graceful = shutdown.clone();
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                shutdown_for_graceful.cancelled().await;
            })
            .await
        {
            warn!(error = %err, "health server failed");
            shutdown.cancel();
        }
    }))
}

async fn ok() -> axum::http::StatusCode {
    axum::http::StatusCode::OK
}

fn map_workflow_error<E>(err: WorkflowError<E>) -> E
where
    E: StdError
        + From<String>
        + From<kube::Error>
        + From<kube_lease_manager::LeaseManagerError>
        + Send
        + Sync
        + 'static,
{
    match err {
        WorkflowError::Graph(err) => E::from(err.to_string()),
        WorkflowError::Reconciler(err) => err,
    }
}

fn map_finalizer_error<E>(err: FinalizerError<E>) -> E
where
    E: StdError
        + From<String>
        + From<kube::Error>
        + From<kube_lease_manager::LeaseManagerError>
        + Send
        + Sync
        + 'static,
{
    match err {
        FinalizerError::ApplyFailed(e) | FinalizerError::CleanupFailed(e) => e,
        FinalizerError::AddFinalizer(e) | FinalizerError::RemoveFinalizer(e) => e.into(),
        other => E::from(other.to_string()),
    }
}
