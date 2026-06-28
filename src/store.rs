use crate::{NamespacedApi, ResourcesExt};
use k8s_openapi::{
    api::core::v1::{ConfigMap, Secret},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use serde::de::Error as _;
use std::collections::BTreeMap;

fn metadata(name: &str, namespace: &str, labels: BTreeMap<String, String>) -> ObjectMeta {
    ObjectMeta {
        name: Some(name.to_string()),
        namespace: Some(namespace.to_string()),
        labels: Some(labels),
        ..Default::default()
    }
}

pub struct StoreHandle {
    name: String,
    data: BTreeMap<String, String>,
}

impl StoreHandle {
    pub fn get(&self, key: impl AsRef<str>) -> Result<&str, kube::Error> {
        let key = key.as_ref();
        self.data.get(key).map(|s| s.as_str()).ok_or_else(|| {
            kube::Error::SerdeError(serde_json::Error::custom(format!(
                "store {} missing key {key}",
                self.name
            )))
        })
    }
}

#[async_trait::async_trait]
pub trait StoreExt {
    async fn store_put(
        &self,
        name: impl AsRef<str> + Send + Sync,
        labels: BTreeMap<String, String>,
        data: BTreeMap<String, String>,
    ) -> Result<(), kube::Error>;

    async fn store_load(
        &self,
        name: impl AsRef<str> + Send + Sync,
    ) -> Result<StoreHandle, kube::Error>;
}

#[async_trait::async_trait]
impl<'a> StoreExt for NamespacedApi<'a, ConfigMap> {
    async fn store_put(
        &self,
        name: impl AsRef<str> + Send + Sync,
        labels: BTreeMap<String, String>,
        data: BTreeMap<String, String>,
    ) -> Result<(), kube::Error> {
        let namespace = self.namespace_str().ok_or_else(|| {
            kube::Error::SerdeError(serde_json::Error::custom(
                "store requires a namespaced handle",
            ))
        })?;
        let config_map = ConfigMap {
            metadata: metadata(name.as_ref(), namespace, labels),
            data: Some(data),
            ..Default::default()
        };
        self.apply(name, &config_map).await
    }

    async fn store_load(
        &self,
        name: impl AsRef<str> + Send + Sync,
    ) -> Result<StoreHandle, kube::Error> {
        let name = name.as_ref();
        let config_map = self.api().get(name).await?;
        let data = config_map.data.ok_or_else(|| {
            kube::Error::SerdeError(serde_json::Error::custom(format!(
                "config map {name} has no data"
            )))
        })?;
        Ok(StoreHandle {
            name: name.to_string(),
            data,
        })
    }
}

#[async_trait::async_trait]
impl<'a> StoreExt for NamespacedApi<'a, Secret> {
    async fn store_put(
        &self,
        name: impl AsRef<str> + Send + Sync,
        labels: BTreeMap<String, String>,
        data: BTreeMap<String, String>,
    ) -> Result<(), kube::Error> {
        let namespace = self.namespace_str().ok_or_else(|| {
            kube::Error::SerdeError(serde_json::Error::custom(
                "store requires a namespaced handle",
            ))
        })?;
        let secret = Secret {
            metadata: metadata(name.as_ref(), namespace, labels),
            string_data: Some(data),
            type_: Some("Opaque".into()),
            ..Default::default()
        };
        self.apply(name, &secret).await
    }

    async fn store_load(
        &self,
        name: impl AsRef<str> + Send + Sync,
    ) -> Result<StoreHandle, kube::Error> {
        let name = name.as_ref();
        let secret = self.api().get(name).await?;
        let raw = secret.data.ok_or_else(|| {
            kube::Error::SerdeError(serde_json::Error::custom(format!(
                "secret {name} has no data"
            )))
        })?;
        let data = raw
            .into_iter()
            .map(|(k, v)| {
                String::from_utf8(v.0)
                    .map_err(|err| {
                        kube::Error::SerdeError(serde_json::Error::custom(format!(
                            "secret {name} key {k} is not valid UTF-8: {err}"
                        )))
                    })
                    .map(|s| (k, s))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(StoreHandle {
            name: name.to_string(),
            data,
        })
    }
}
