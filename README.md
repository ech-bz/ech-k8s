# ech-k8s

DAG-based Kubernetes operator toolkit for `kube-rs`.

- DAG reconciliation with dependency tracking and cycle detection
- Leader election via `kube-lease-manager`
- Finalizer lifecycle (apply / cleanup)
- Status subresource patching with phase and progress
- Health probes and graceful shutdown
- Server-side apply, prune, and patch helpers via `ResourcesExt`
- ConfigMap and Secret store via `StoreExt`

## Usage

**Define a workflow:**
```rust
use ech_k8s::{Graph, GraphError, Workflow};

struct MyOperator;

impl Workflow for MyOperator {
    type Crd = MyCrd;
    type Error = MyError;

    fn build_graph(&self, _cr: &MyCrd) -> Result<Graph<Self::Crd, Self::Error>, GraphError> {
        let mut graph = Graph::new();
        graph.add(Prune, vec![])?;
        graph.add(Setup, vec![Prune.into()])?;
        graph.add(Deploy, vec![Setup.into()])?;
        Ok(graph)
    }
}
```

**Write a reconciler:**
```rust
use ech_k8s::{K8sClient, NodeState, Reconciler, ResourcesExt, StoreExt};
use k8s_openapi::api::core::v1::ConfigMap;

#[async_trait]
impl Reconciler for Setup {
    type Crd = MyCrd;
    type Error = MyError;

    async fn reconcile(&self, client: &K8sClient, cr: &MyCrd) -> Result<NodeState, MyError> {
        let ns = cr.cr_ns()?;

        client.namespaced::<ConfigMap>(&ns)
            .store_put("my-config", "key", "value", Some(labels))
            .await?;

        client.namespaced::<MyCrd>(&ns)
            .patch_status("my-cr", &status)
            .await?;

        Ok(NodeState::Ready)
    }
}
```

**Run the operator:**
```rust
use ech_k8s::{Operator, OperatorSpec, RuntimeSettings};

#[tokio::main]
async fn main() -> Result<(), MyError> {
    Operator::run(OperatorSpec {
        runtime: RuntimeSettings {
            health_port: 8080,
            reconcile_interval: Duration::from_secs(30),
            error_backoff: Duration::from_secs(10),
        },
        leader: config.leader,
        field_manager: "my-operator",
        workflow: MyOperator,
    })
    .await
}
```
