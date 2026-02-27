use std::{borrow::Cow, num::NonZeroU64};

use serde::{Deserialize, Serialize};
use zarrs::{
    array::{
        ArrayMetadataV3, FillValueMetadata,
        chunk_grid::{RegularBoundedChunkGrid, RegularBoundedChunkGridConfiguration},
        data_type,
    },
    group::GroupMetadataV3,
    metadata::v3::{MetadataV3, NodeMetadataV3},
    plugin::{ExtensionAliasesV3, ExtensionName, ZarrVersion},
};
use zarrs_codec::CodecTraits;

use crate::{chunk_key_encoding::N5ChunkKeyEncoding, codec::N5Codec};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum N5Metadata {
    Array(N5ArrayMetadata),
    Group(N5GroupMetadata),
}

impl N5Metadata {
    pub fn version(&self) -> Option<&str> {
        match self {
            N5Metadata::Array(m) => m.version.as_deref(),
            N5Metadata::Group(m) => m.version.as_deref(),
        }
    }

    pub fn into_attributes(self) -> serde_json::Map<String, serde_json::Value> {
        match self {
            N5Metadata::Array(m) => m.attributes,
            N5Metadata::Group(m) => m.attributes,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N5GroupMetadata {
    #[serde(rename = "n5")]
    pub version: Option<String>,
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct N5ArrayMetadata {
    #[serde(rename = "n5")]
    pub version: Option<String>,
    pub dimensions: Vec<u64>,
    pub block_size: Vec<u64>,
    pub data_type: String,
    pub compression: N5Compression,
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Copy)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum N5Compression {
    #[default]
    Raw,
    Bzip2 {
        /// Default 9. Must be in the range 1..=9.
        #[serde(default = "default_bzip2_block_size")]
        block_size: u8,
    },
    Gzip {
        /// Default -1, meaning "implementation default" (usually 6).
        #[serde(default = "default_gzip_level")]
        level: i8,
    },
    Lz4 {
        /// Default 65536. Must be a positive integer.
        #[serde(default = "lz4_default_level")]
        level: u64,
    },
    Xz {
        /// Default 6.
        #[serde(default = "default_xz_preset")]
        preset: u32,
    },
    // https://github.com/saalfeldlab/n5-blosc
    // https://github.com/JaneliaSciComp/n5-zstandard/
}

fn default_bzip2_block_size() -> u8 {
    9
}

fn default_gzip_level() -> i8 {
    -1
}

fn lz4_default_level() -> u64 {
    65536
}

fn default_xz_preset() -> u32 {
    6
}

/// Reverses block_size and creates regular chunk grid
fn convert_chunk_grid(block_size: &[u64]) -> crate::Result<MetadataV3> {
    let chunk_shape: Vec<_> = block_size
        .iter()
        .map(|&n| NonZeroU64::new(n).ok_or_else(|| crate::Error::general("zero block size")))
        .rev()
        .collect::<crate::Result<Vec<_>>>()?;
    let out = MetadataV3::new_with_serializable_configuration(
        RegularBoundedChunkGrid::aliases_v3()
            .default_name
            .clone()
            .to_string(),
        &RegularBoundedChunkGridConfiguration { chunk_shape },
    )?;

    Ok(out)
}

fn convert_data_type(data_type: &str) -> crate::Result<MetadataV3> {
    let data_type = match data_type {
        "uint8" => data_type::uint8(),
        "int8" => data_type::int8(),
        "int16" => data_type::int16(),
        "uint16" => data_type::uint16(),
        "int32" => data_type::int32(),
        "uint32" => data_type::uint32(),
        "int64" => data_type::int64(),
        "uint64" => data_type::uint64(),
        "float32" => data_type::float32(),
        "float64" => data_type::float64(),
        s => return Err(crate::Error::general(format!("unsupported data type: {s}"))),
    };
    let data_type_name = data_type
        .name_v3()
        .map_or_else(String::new, Cow::into_owned);
    let data_type_configuration = data_type.configuration_v3();
    let out = if data_type_configuration.is_empty() {
        MetadataV3::new(data_type_name)
    } else {
        MetadataV3::new_with_configuration(data_type_name, data_type_configuration)
    };
    Ok(out)
}

fn convert_fill_value() -> FillValueMetadata {
    FillValueMetadata::Number(serde_json::Number::from(0))
}

fn convert_chunk_key_encoding() -> MetadataV3 {
    MetadataV3::new(
        N5ChunkKeyEncoding::aliases_v3()
            .default_name
            .clone()
            .to_string(),
    )
}

impl TryFrom<N5ArrayMetadata> for ArrayMetadataV3 {
    type Error = crate::Error;

    fn try_from(value: N5ArrayMetadata) -> Result<Self, Self::Error> {
        let shape: Vec<_> = value.dimensions.iter().rev().copied().collect();
        let chunk_grid = convert_chunk_grid(&value.block_size)?;
        let data_type = convert_data_type(&value.data_type)?;
        let fill_value = convert_fill_value();

        let zarr_version = ZarrVersion::V3;
        let n5_codec = N5Codec::new(value.compression)?;
        let name = n5_codec
            .name(zarr_version)
            .unwrap_or_else(|| "zarrs.n5".into());
        let codec_meta = if let Some(config) =
            n5_codec.configuration(zarr_version, &zarrs_codec::CodecMetadataOptions::default())
        {
            MetadataV3::new_with_configuration(name, config)
        } else {
            MetadataV3::new(name)
        };
        let out = Self::new(shape, chunk_grid, data_type, fill_value, vec![codec_meta])
            .with_chunk_key_encoding(convert_chunk_key_encoding())
            .with_attributes(value.attributes);
        Ok(out)
    }
}

impl TryFrom<N5Metadata> for NodeMetadataV3 {
    type Error = crate::Error;

    fn try_from(value: N5Metadata) -> Result<Self, Self::Error> {
        match value {
            N5Metadata::Array(m) => ArrayMetadataV3::try_from(m).map(Self::Array),
            N5Metadata::Group(m) => Ok(Self::Group(
                GroupMetadataV3::default().with_attributes(m.attributes),
            )),
        }
    }
}
