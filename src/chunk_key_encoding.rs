use std::fmt::Write;

use zarrs::{
    array::{
        ChunkKeyEncoding, ChunkKeyEncodingTraits,
        chunk_key_encoding::{self as cke, api::ChunkKeyEncodingPlugin},
    },
    plugin::PluginConfigurationInvalidError,
};

#[derive(Debug, Clone, Copy)]
pub struct N5ChunkKeyEncoding;

zarrs::plugin::impl_extension_aliases!(N5ChunkKeyEncoding, v3: "zarrs.n5", ["zarrs.n5", "n5"]);
inventory::submit! {
    ChunkKeyEncodingPlugin::new::<N5ChunkKeyEncoding>()
}

impl ChunkKeyEncodingTraits for N5ChunkKeyEncoding {
    fn create(
        metadata: &zarrs::metadata::v3::MetadataV3,
    ) -> Result<cke::api::ChunkKeyEncoding, zarrs::plugin::PluginCreateError>
    where
        Self: Sized,
    {
        let cke = match metadata.name() {
            "zarrs.n5" | "n5" => ChunkKeyEncoding::new(Self),
            _ => {
                return Err(zarrs::plugin::PluginCreateError::NameInvalid {
                    name: metadata.name().into(),
                });
            }
        };
        if !metadata.configuration_is_none_or_empty() {
            return Err(zarrs::plugin::PluginCreateError::ConfigurationInvalid(
                PluginConfigurationInvalidError::new(
                    "N5 chunk key encoding does not support configuration".into(),
                ),
            ));
        }
        Ok(cke)
    }

    fn configuration(&self) -> zarrs::metadata::Configuration {
        Default::default()
    }

    fn encode(&self, chunk_grid_indices: &[u64]) -> zarrs::storage::StoreKey {
        let mut s = String::with_capacity(chunk_grid_indices.len() * 2);
        let mut is_first = true;
        for idx in chunk_grid_indices.iter().rev() {
            if is_first {
                is_first = false;
            } else {
                s.push('/');
            }
            s.write_fmt(format_args!("{idx}")).unwrap();
        }
        zarrs::storage::StoreKey::new(s).expect("chunk key should be valid")
    }
}
