use std::{borrow::Cow, num::NonZeroU64, sync::Arc};

use serde::{Deserialize, Serialize};
use zarrs::{
    array::{
        ArrayMetadataV3, BytesToBytesCodecTraits, ChunkKeyEncodingTraits, CodecMetadataOptions,
        FillValueMetadata,
        chunk_grid::{RegularBoundedChunkGrid, RegularBoundedChunkGridConfiguration},
        chunk_key_encoding::V2ChunkKeyEncoding,
        codec::{
            BloscCodec, BloscCompressionLevel, BloscCompressor, BloscShuffleMode, Bz2Codec,
            Bz2CompressionLevel, GzipCodec, ZstdCodec, api::CodecTraits,
        },
        data_type,
    },
    group::GroupMetadataV3,
    metadata::v3::{MetadataV3, NodeMetadataV3},
    plugin::{ExtensionAliasesV3, ExtensionName, ZarrVersion},
};

use crate::{codec::N5DefaultCodec, storage::N5ArrayMode};

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

    /// Whether this metadata represents a hierarchy root, i.e. has an N5 version.
    pub fn is_root(&self) -> bool {
        self.version().is_some()
    }

    /// Extract the unstructured attributes map.
    pub fn into_attributes(self) -> serde_json::Map<String, serde_json::Value> {
        match self {
            N5Metadata::Array(m) => m.attributes,
            N5Metadata::Group(m) => m.attributes,
        }
    }

    pub fn try_into_zarr(self, array_mode: N5ArrayMode) -> crate::Result<NodeMetadataV3> {
        match self {
            N5Metadata::Array(m) => m.try_into_zarr(array_mode).map(NodeMetadataV3::Array),
            N5Metadata::Group(m) => Ok(NodeMetadataV3::Group(m.into())),
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
    /// Array shape.
    pub dimensions: Vec<u64>,
    /// Chunk shape.
    pub block_size: Vec<NonZeroU64>,
    /// Data type as a string.
    pub data_type: String,
    /// Chunk compression configuration.
    pub compression: N5Compression,
    /// Unstructured attributes.
    #[serde(flatten)]
    pub attributes: serde_json::Map<String, serde_json::Value>,
}

impl N5ArrayMetadata {
    /// Try to convert the N5 metadata to Zarr metadata using the given array mode.
    ///
    /// Only the 'default' array mode is currently supported.
    pub fn try_into_zarr(self, array_mode: N5ArrayMode) -> crate::Result<ArrayMetadataV3> {
        let ser_val = serde_json::to_value(self.clone())?;
        let mut attrs = self.attributes;
        attrs.insert("_n5".into(), ser_val);

        let shape: Vec<_> = self.dimensions;

        let zarr_version = ZarrVersion::V3;
        let codec_meta = match array_mode {
            N5ArrayMode::Default => {
                let n5_codec =
                    N5DefaultCodec::new(self.compression.to_bytes_to_bytes_codec()?, shape.len());
                let name = n5_codec
                    .name(zarr_version)
                    .unwrap_or_else(|| "zarrs.n5_default".into());
                if let Some(config) =
                    n5_codec.configuration(zarr_version, &CodecMetadataOptions::default())
                {
                    MetadataV3::new_with_configuration(name, config)
                } else {
                    MetadataV3::new(name)
                }
            }
            _ => {
                return Err(crate::Error::general(format!(
                    "N5 array mode {array_mode:?} is not compatible with configured array mode {array_mode:?}"
                )));
            }
        };

        let chunk_grid = convert_chunk_grid(&self.block_size)?;
        let data_type = convert_data_type(&self.data_type)?;
        let fill_value = convert_fill_value();

        let out = ArrayMetadataV3::new(shape, chunk_grid, data_type, fill_value, vec![codec_meta])
            .with_chunk_key_encoding(convert_chunk_key_encoding())
            .with_attributes(attrs);
        Ok(out)
    }
}

/// N5 block compression configuration.
#[non_exhaustive]
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
    /// <https://github.com/JaneliaSciComp/n5-zstd>
    Zstd {
        /// Default 3. Must be in the range -5..=22.
        #[serde(default = "default_zstd_level")]
        level: i32,
    },
    /// <https://github.com/saalfeldlab/n5-blosc>
    Blosc {
        /// Compressor name
        #[serde(default = "default_blosc_cname")]
        cname: BloscCompressor,
        /// Compressor level
        #[serde(default = "default_blosc_clevel")]
        clevel: BloscCompressionLevel,
        /// - -1: auto
        /// - 0: no shuffle
        /// - 1: byte shuffle
        /// - 2: bit shuffle
        #[serde(default = "default_blosc_shuffle")]
        shuffle: i32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        blocksize: Option<usize>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        typesize: Option<usize>,
        #[serde(default = "default_blosc_nthreads")]
        nthreads: u32,
    },
}

fn default_blosc_cname() -> BloscCompressor {
    BloscCompressor::BloscLZ
}

fn default_blosc_clevel() -> BloscCompressionLevel {
    BloscCompressionLevel::try_from(6).unwrap()
}

fn default_blosc_shuffle() -> i32 {
    0
}

fn default_blosc_nthreads() -> u32 {
    1
}

fn default_zstd_level() -> i32 {
    3
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
    ) -> crate::Result<Option<Arc<dyn BytesToBytesCodecTraits>>> {
        let b2b: Arc<dyn BytesToBytesCodecTraits> = match self {
            N5Compression::Raw => return Ok(None),
            N5Compression::Bzip2 { block_size } => Arc::new(Bz2Codec::new(
                Bz2CompressionLevel::new(*block_size as u32)
                    .map_err(|n| crate::Error::general(format!("invalid bz2 block size {n}")))?,
            )),
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
                Arc::new(GzipCodec::new(lvl_int).map_err(crate::Error::wrap)?)
            }
            N5Compression::Zstd { level } => {
                // TODO: checksum?
                Arc::new(ZstdCodec::new(*level, false))
            }
            N5Compression::Blosc {
                cname,
                clevel,
                shuffle,
                blocksize,
                typesize,
                ..
            } => {
                let shuffle_mode = match *shuffle {
                    -1 => {
                        if typesize.unwrap_or(1) > 1 {
                            BloscShuffleMode::Shuffle
                        } else {
                            BloscShuffleMode::NoShuffle
                        }
                    }
                    0 => BloscShuffleMode::NoShuffle,
                    1 => BloscShuffleMode::Shuffle,
                    2 => BloscShuffleMode::BitShuffle,
                    n => {
                        return Err(crate::Error::general(format!(
                            "invalid Blosc shuffle mode {n}"
                        )));
                    }
                };
                Arc::new(
                    BloscCodec::new(*cname, *clevel, *blocksize, shuffle_mode, *typesize).map_err(
                        |e| crate::Error::general(format!("invalid Blosc configuration: {e}")),
                    )?,
                )
            }
            // N5Compression::Lz4 { level } => todo!(),
            // N5Compression::Xz { preset } => todo!(),
            c => {
                return Err(crate::Error::general(format!(
                    "unsupported N5 compression: {c:?}"
                )));
            }
        };
        Ok(Some(b2b))
    }
}

fn convert_chunk_grid(block_size: &[NonZeroU64]) -> crate::Result<MetadataV3> {
    let chunk_shape: Vec<_> = block_size.to_vec();
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
    let cke = V2ChunkKeyEncoding::new_slash();
    MetadataV3::new_with_configuration(
        cke.name_v3()
            .expect("v2 chunk key encoding should have name"),
        cke.configuration(),
    )
}

impl From<N5GroupMetadata> for GroupMetadataV3 {
    fn from(value: N5GroupMetadata) -> Self {
        let ser_val =
            serde_json::to_value(value.clone()).expect("N5 group metadata should be serializable");
        let mut attrs = value.attributes;
        attrs.insert("_n5".into(), ser_val);
        Self::default().with_attributes(attrs)
    }
}
