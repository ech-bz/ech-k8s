use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::{any::TypeId, error::Error as StdError};
use thiserror::Error;

use crate::K8sClient;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NodeKey {
    pub kind: TypeId,
    pub data: String,
}

impl NodeKey {
    pub fn from_value<T>(value: &T) -> Self
    where
        T: Serialize + 'static,
    {
        Self {
            kind: TypeId::of::<T>(),
            data: serde_json::to_string(value).expect("failed to serialize node key payload"),
        }
    }
}

impl<T> From<T> for NodeKey
where
    T: Serialize + 'static,
{
    fn from(value: T) -> Self {
        Self::from_value(&value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
    Ready,
    Pending,
}

#[async_trait]
pub trait Reconciler: Send + Sync + 'static {
    type Crd: Send + Sync + 'static;
    type Error: StdError + Send + Sync + 'static;

    async fn reconcile(&self, client: &K8sClient, cr: &Self::Crd)
    -> Result<NodeState, Self::Error>;

    async fn cleanup(&self, client: &K8sClient, cr: &Self::Crd) -> Result<(), Self::Error> {
        let _ = (client, cr);
        Ok(())
    }
}

pub trait Component: Clone + Serialize + Send + Sync + 'static {
    const NAME: &'static str;

    fn instance_name(&self, owner: impl AsRef<str>) -> Result<String, ReconcilerMetaError>;

    fn labels(
        &self,
        owner: impl AsRef<str>,
    ) -> Result<BTreeMap<String, String>, ReconcilerMetaError>;

    fn selector(owner: impl AsRef<str>) -> String;
}

#[derive(Debug, Error)]
#[error("failed to serialize for component meta: {0}")]
pub struct ReconcilerMetaError(#[from] serde_json::Error);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phase: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ready: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_nodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_nodes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_nodes: Option<i64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pending: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("duplicate node declared: {0:?}")]
    DuplicateNode(NodeKey),
    #[error("missing dependency {dependency:?} for node {node:?}")]
    MissingDependency { node: NodeKey, dependency: NodeKey },
    #[error("cycle detected at node: {0:?}")]
    CycleDetected(NodeKey),
}

#[derive(Debug, Error)]
pub enum WorkflowError<E: StdError + Send + Sync + 'static> {
    #[error(transparent)]
    Graph(#[from] GraphError),
    #[error("reconciler failed: {0}")]
    Reconciler(#[source] E),
}

pub struct Graph<CR, E>
where
    CR: Send + Sync + 'static,
    E: StdError + Send + Sync + 'static,
{
    nodes: Vec<(
        NodeKey,
        Box<dyn Reconciler<Crd = CR, Error = E>>,
        Vec<NodeKey>,
    )>,
}

impl<CR, E> Graph<CR, E>
where
    CR: Send + Sync + 'static,
    E: StdError + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn add<T>(
        &mut self,
        node: T,
        deps: impl IntoIterator<Item = NodeKey>,
    ) -> Result<(), GraphError>
    where
        T: Reconciler<Crd = CR, Error = E> + Clone + Serialize + Send + Sync + 'static,
    {
        let key = node.clone().into();
        if self.nodes.iter().any(|(k, _, _)| k == &key) {
            return Err(GraphError::DuplicateNode(key));
        }
        self.nodes
            .push((key, Box::new(node), deps.into_iter().collect()));
        Ok(())
    }
}

pub struct Scheduler<CR, E>
where
    CR: Send + Sync + 'static,
    E: StdError + Send + Sync + 'static,
{
    g: petgraph::graph::Graph<NodeKey, ()>,
    records: HashMap<NodeKey, Box<dyn Reconciler<Crd = CR, Error = E>>>,
    indices: HashMap<NodeKey, petgraph::graph::NodeIndex>,
}

impl<CR, E> Scheduler<CR, E>
where
    CR: Send + Sync + 'static,
    E: StdError + Send + Sync + 'static,
{
    pub(crate) fn new(graph: Graph<CR, E>) -> Result<Self, GraphError> {
        for (key, _, deps) in &graph.nodes {
            for dep in deps {
                if !graph.nodes.iter().any(|(k, _, _)| k == dep) {
                    return Err(GraphError::MissingDependency {
                        node: key.clone(),
                        dependency: dep.clone(),
                    });
                }
            }
        }

        let mut g = petgraph::graph::Graph::new();
        let mut records = HashMap::new();
        let mut indices = HashMap::new();

        for (key, reconciler, deps) in graph.nodes {
            let node_idx = g.add_node(key.clone());
            indices.insert(key.clone(), node_idx);
            records.insert(key.clone(), reconciler);

            for dep in deps {
                let dep_idx = *indices
                    .entry(dep.clone())
                    .or_insert_with(|| g.add_node(dep));
                g.add_edge(dep_idx, node_idx, ());
            }
        }

        petgraph::algo::toposort(&g, None).map_err(|_| {
            let cycle_node = g.node_weights().next().expect("non-empty graph").clone();
            GraphError::CycleDetected(cycle_node)
        })?;

        Ok(Self {
            g,
            records,
            indices,
        })
    }

    fn runnable(&self, completed: &HashSet<NodeKey>, pending: &HashSet<NodeKey>) -> Vec<NodeKey> {
        self.records
            .keys()
            .filter(|id| {
                if completed.contains(*id) || pending.contains(*id) {
                    return false;
                }
                let &idx = self
                    .indices
                    .get(*id)
                    .expect("record key must have graph index");
                self.g
                    .neighbors_directed(idx, petgraph::Incoming)
                    .all(|dep_idx| completed.contains(&self.g[dep_idx]))
            })
            .cloned()
            .collect()
    }

    pub async fn run(
        &self,
        client: &K8sClient,
        cr: &CR,
        observed_generation: Option<i64>,
    ) -> Result<WorkflowStatus, WorkflowError<E>> {
        let mut completed: HashSet<NodeKey> = HashSet::new();
        let mut pending: HashSet<NodeKey> = HashSet::new();

        loop {
            let frontier = self.runnable(&completed, &pending);
            let mut progressed = false;
            for node_id in frontier {
                let node_state = self
                    .records
                    .get(&node_id)
                    .expect("frontier node should exist in graph")
                    .reconcile(client, cr)
                    .await
                    .map_err(WorkflowError::Reconciler)?;

                match node_state {
                    NodeState::Ready => {
                        progressed = true;
                        completed.insert(node_id.clone());
                        pending.remove(&node_id);
                    }
                    NodeState::Pending => {
                        pending.insert(node_id.clone());
                    }
                }
            }

            if !progressed {
                break;
            }
        }

        let total_nodes = self.records.len() as i64;
        let n_completed = completed.len() as i64;

        let uncompleted: HashSet<&NodeKey> = self
            .records
            .keys()
            .filter(|n| !completed.contains(*n))
            .collect();
        let n_pending = uncompleted.len() as i64;

        let mut pending_vec: Vec<String> = uncompleted.iter().map(|n| format!("{n:?}")).collect();
        pending_vec.sort();

        let (phase, ready, message) = if n_pending == 0 {
            (
                Some("Ready".to_string()),
                Some(true),
                Some("all workflow nodes are ready".to_string()),
            )
        } else if n_completed == 0 {
            (
                Some("Pending".to_string()),
                Some(false),
                Some("waiting for workflow dependencies".to_string()),
            )
        } else {
            (
                Some("Progressing".to_string()),
                Some(false),
                Some(format!(
                    "{n_completed} of {total_nodes} workflow nodes are ready"
                )),
            )
        };

        Ok(WorkflowStatus {
            observed_generation,
            phase,
            ready,
            total_nodes: Some(total_nodes),
            completed_nodes: Some(n_completed),
            pending_nodes: Some(n_pending),
            pending: pending_vec,
            message,
        })
    }

    pub async fn cleanup(&self, client: &K8sClient, cr: &CR) -> Result<(), WorkflowError<E>> {
        let order = petgraph::algo::toposort(&self.g, None)
            .map(|indices| {
                indices
                    .into_iter()
                    .map(|i| self.g[i].clone())
                    .collect::<Vec<_>>()
            })
            .map_err(|_| {
                GraphError::CycleDetected(
                    self.g
                        .node_weights()
                        .next()
                        .expect("non-empty graph")
                        .clone(),
                )
            })?;

        for node_id in order.into_iter().rev() {
            self.records
                .get(&node_id)
                .expect("topological node should exist in graph")
                .cleanup(client, cr)
                .await
                .map_err(WorkflowError::Reconciler)?;
        }

        Ok(())
    }
}

pub trait Workflow: Send + Sync + 'static {
    type Crd: Send + Sync + 'static;
    type Error: StdError + Send + Sync + 'static;

    fn build_graph(&self, cr: &Self::Crd) -> Result<Graph<Self::Crd, Self::Error>, GraphError>;
}
