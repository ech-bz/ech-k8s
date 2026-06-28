mod leader;
pub mod operator;
pub mod resources;
pub mod store;
pub mod workflow;

pub use leader::LeaderSettings;
pub use operator::{Operator, OperatorSpec, RuntimeSettings};
pub use resources::ResourcesExt;
pub use store::{StoreExt, StoreHandle};
pub use workflow::{
    Component, Graph, GraphError, NodeKey, NodeState, Reconciler, ReconcilerMetaError, Scheduler,
    Workflow, WorkflowError,
};

pub use ech_k8s_derive::Component;

use kube::api::Api;
use thiserror::Error;

#[derive(Clone)]
pub struct K8sClient {
    pub(crate) field_manager: String,
    pub(crate) client: kube::Client,
}

impl std::fmt::Debug for K8sClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("K8sClient")
            .field("field_manager", &self.field_manager)
            .finish_non_exhaustive()
    }
}

impl K8sClient {
    pub async fn try_new(field_manager: impl Into<String>) -> Result<Self, kube::Error> {
        Ok(Self {
            field_manager: field_manager.into(),
            client: kube::Client::try_default().await?,
        })
    }

    pub fn client(&self) -> &kube::Client {
        &self.client
    }

    pub fn field_manager(&self) -> &str {
        &self.field_manager
    }

    pub fn all<T>(&self) -> NamespacedApi<'_, T>
    where
        T: kube::Resource,
        <T as kube::Resource>::DynamicType: Default,
    {
        NamespacedApi {
            client: self,
            api: Api::all(self.client.clone()),
            namespace: None,
        }
    }

    pub fn namespaced<T>(&self, namespace: impl AsRef<str>) -> NamespacedApi<'_, T>
    where
        T: kube::Resource<Scope = kube::core::NamespaceResourceScope>,
        <T as kube::Resource>::DynamicType: Default,
    {
        NamespacedApi {
            client: self,
            api: Api::namespaced(self.client.clone(), namespace.as_ref()),
            namespace: Some(namespace.as_ref().to_string()),
        }
    }

    pub fn default_namespaced<T>(&self) -> NamespacedApi<'_, T>
    where
        T: kube::Resource<Scope = kube::core::NamespaceResourceScope>,
        <T as kube::Resource>::DynamicType: Default,
    {
        NamespacedApi {
            client: self,
            api: Api::default_namespaced(self.client.clone()),
            namespace: Some(self.client.default_namespace().to_string()),
        }
    }
}

pub struct NamespacedApi<'a, T> {
    pub(crate) client: &'a K8sClient,
    pub(crate) api: Api<T>,
    pub(crate) namespace: Option<String>,
}

impl<'a, T> NamespacedApi<'a, T> {
    pub fn api(&self) -> &Api<T> {
        &self.api
    }

    pub fn into_inner(self) -> Api<T> {
        self.api
    }

    pub fn namespace_str(&self) -> Option<&str> {
        self.namespace.as_deref()
    }
}

#[derive(Debug, Error)]
pub enum CrMetaError {
    #[error("CR is missing required metadata field: namespace")]
    MissingNamespace,
    #[error("CR is missing required metadata field: name")]
    MissingName,
}

pub trait CrMeta {
    fn cr_ns(&self) -> Result<String, CrMetaError>;
    fn cr_name(&self) -> Result<String, CrMetaError>;
}

impl<T> CrMeta for T
where
    T: kube::Resource<Scope = kube::core::NamespaceResourceScope>,
{
    fn cr_ns(&self) -> Result<String, CrMetaError> {
        <Self as kube::ResourceExt>::namespace(self).ok_or(CrMetaError::MissingNamespace)
    }

    fn cr_name(&self) -> Result<String, CrMetaError> {
        <Self as kube::Resource>::meta(self)
            .name
            .clone()
            .ok_or(CrMetaError::MissingName)
    }
}
