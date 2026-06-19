use kube::{
    Resource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, fmt::Debug, hash::Hash};

use crate::NamespacedApi;

#[async_trait::async_trait]
pub trait ResourcesExt<T>: Sized {
    async fn apply(
        &self,
        name: impl AsRef<str> + Send + Sync,
        resource: &T,
    ) -> Result<(), kube::Error>;

    async fn prune(
        &self,
        label_selector: impl AsRef<str> + Send + Sync,
        desired: &BTreeSet<String>,
    ) -> Result<(), kube::Error>;

    async fn patch_status<P>(&self, name: &str, status: &P) -> Result<(), kube::Error>
    where
        P: Serialize + Debug + Send + Sync;

    async fn delete_if_exists(
        &self,
        name: impl AsRef<str> + Send + Sync,
    ) -> Result<(), kube::Error>;
}

#[async_trait::async_trait]
impl<'a, T> ResourcesExt<T> for NamespacedApi<'a, T>
where
    T: Resource + Clone + Debug + Send + Sync + Serialize + DeserializeOwned,
    <T as Resource>::DynamicType: Default + Eq + Hash + Clone,
{
    async fn apply(
        &self,
        name: impl AsRef<str> + Send + Sync,
        resource: &T,
    ) -> Result<(), kube::Error> {
        let params = PatchParams::apply(self.client.field_manager()).force();
        self.api
            .patch(name.as_ref(), &params, &Patch::Apply(resource))
            .await?;
        Ok(())
    }

    async fn prune(
        &self,
        label_selector: impl AsRef<str> + Send + Sync,
        desired: &BTreeSet<String>,
    ) -> Result<(), kube::Error> {
        let lp = ListParams::default().labels(label_selector.as_ref());
        for cr in self.api.list(&lp).await?.items {
            let name = cr.name_any();
            if !desired.contains(name.as_str()) {
                tracing::info!(name = %name, kind = std::any::type_name::<T>(), "deleting stale resource");
                if let Err(err) = self.delete_if_exists(&name).await {
                    tracing::warn!(name = %name, error = %err, "stale resource delete failed");
                }
            }
        }
        Ok(())
    }

    async fn patch_status<P>(&self, name: &str, status: &P) -> Result<(), kube::Error>
    where
        P: Serialize + Debug + Send + Sync,
    {
        #[derive(Debug, Serialize)]
        struct StatusPatch<'a, P> {
            status: &'a P,
        }

        let body = StatusPatch { status };
        let params = PatchParams::default();
        self.api
            .patch_status(name, &params, &Patch::Merge(&body))
            .await?;
        Ok(())
    }

    async fn delete_if_exists(
        &self,
        name: impl AsRef<str> + Send + Sync,
    ) -> Result<(), kube::Error> {
        match self
            .api
            .delete(name.as_ref(), &DeleteParams::foreground())
            .await
        {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(ref status)) if status.code == 404 => Ok(()),
            Err(err) => Err(err),
        }
    }
}
