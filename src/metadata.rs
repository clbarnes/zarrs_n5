use std::{borrow::Cow, num::NonZeroU64, sync::Arc};

use serde::{Deserialize, Serialize};
use zarrs::{
    array::{
        ArrayMetadataV3, FillValueMetadata,
        chunk_grid::{RegularBoundedChunkGrid, RegularBoundedChunkGridConfiguration},
        codec::{Bz2Codec, Bz2CompressionLevel, GzipCodec},
        data_type,
    },
    group::GroupMetadataV3,
    metadata::v3::{MetadataV3, NodeMetadataV3},
    plugin::{ExtensionAliasesV3, ExtensionName, ZarrVersion},
};
use zarrs_codec::CodecTraits;

use crate::{chunk_key_encoding::N5ChunkKeyEncoding, codec::N5Codec};

/// Representation of N5 metadata, either an array or a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum N5Metadata {
    Array(N5ArrayMetadata),
    Group(N5GroupMetadata),
}

impl From<N5ArrayMetadata> for N5Metadata {
    fn from(value: N5ArrayMetadata) -> Self {
        Self::Array(value)
    }
}

impl From<N5GroupMetadata> for N5Metadata {
    fn from(value: N5GroupMetadata) -> Self {
        Self::Group(value)
    }
}

impl N5Metadata {
    /// Get the N5 version if present.
    pub fn version(&self) -> Option<&str> {
        match self {
            N5Metadata::Array(m) => m.n5_version.as_deref(),
            N5Metadata::Group(m) => m.n5_version.as_deref(),
        }
    }

    /// Extract the unstructured attributes map.
    pub fn into_attributes(self) -> serde_json::Map<String, serde_json::Value> {
        match self {
            N5Metadata::Array(m) => m.attributes,
            N5Metadata::Group(m) => m.attributes,
        }
    }
}

/// Representation of N5 group metadata.
///
/// Should be deserialized via the [N5Metadata] enum,
/// as all N5 arrays are also groups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N5GroupMetadata {
    /// N5 version; present if this is a hierarchy root.
    #[serde(rename = "n5")]
    pub n5_version: Option<String>,
    /// Unstructured attributes.
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

/// Representation of N5 array metadata.
///
/// Should be deserialized via the [N5Metadata] enum,
/// as all N5 arrays are also groups.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct N5ArrayMetadata {
    /// N5 version; present if this is a hierarchy root.
    #[serde(rename = "n5")]
    pub n5_version: Option<String>,
    /// Array shape. Note that N5 uses F order, so the dimensions are reversed compared to Zarr.
    pub dimensions: Vec<u64>,
    /// Chunk shape. Note that N5 uses F order, so the dimensions are reversed compared to Zarr.
    pub block_size: Vec<u64>,
    /// Data type as a string.
    pub data_type: String,
    /// Chunk compression configuration.
    pub compression: N5Compression,
    /// Unstructured attributes.
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

/// N5 chunk compression configuration.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Copy)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum N5Compression {
    /// Uncompressed.
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
    // TODO https://github.com/saalfeldlab/n5-blosc
    // TODO https://github.com/JaneliaSciComp/n5-zstandard/
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

impl N5Compression {
    /// Convert to a bytes-to-bytes codec if possible.
    pub fn to_bytes_to_bytes_codec(
        &self,
    ) -> crate::Result<Option<Arc<dyn zarrs_codec::BytesToBytesCodecTraits>>> {
        match self {
            N5Compression::Raw => Ok(None),
            N5Compression::Bzip2 { block_size } => Ok(Some(Arc::new(Bz2Codec::new(
                Bz2CompressionLevel::new(*block_size as u32)
                    .map_err(|n| crate::Error::general(format!("invalid bz2 block size {n}")))?,
            )))),
            N5Compression::Gzip { level } => {
                let lvl_int: u32 = match level {
                    -1 => 6,
                    n if *n >= 0 => *n as u32,
                    n => {
                        return Err(crate::Error::general(format!(
                            "invalid gzip compression level {n}"
                        )));
                    }
                };
                Ok(Some(Arc::new(
                    GzipCodec::new(lvl_int).map_err(crate::Error::wrap)?,
                )))
            }
            // N5Compression::Lz4 { level } => todo!(),
            // N5Compression::Xz { preset } => todo!(),
            c => Err(crate::Error::general(format!(
                "unsupported N5 compression: {c:?}"
            ))),
        }
    }
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

impl From<N5GroupMetadata> for GroupMetadataV3 {
    fn from(value: N5GroupMetadata) -> Self {
        Self::default().with_attributes(value.attributes)
    }
}

impl TryFrom<N5ArrayMetadata> for ArrayMetadataV3 {
    type Error = crate::Error;

    fn try_from(value: N5ArrayMetadata) -> Result<Self, Self::Error> {
        let shape: Vec<_> = value.dimensions.iter().rev().copied().collect();
        let chunk_grid = convert_chunk_grid(&value.block_size)?;
        let data_type = convert_data_type(&value.data_type)?;
        let fill_value = convert_fill_value();

        let zarr_version = ZarrVersion::V3;
        let n5_codec = N5Codec::new(value.compression.to_bytes_to_bytes_codec()?);
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
            N5Metadata::Array(m) => m.try_into().map(Self::Array),
            N5Metadata::Group(m) => Ok(Self::Group(m.into())),
        }
    }
}
