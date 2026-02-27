use std::borrow::Cow;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zarrs::array::CodecChain;
use zarrs::array::codec::BytesCodec;
use zarrs::array::codec::bytes_to_bytes::gzip::GzipCodec;
use zarrs::array::codec::{Bz2CompressionLevel, bytes_to_bytes::bz2::Bz2Codec};
use zarrs::metadata::v3::MetadataV3;
use zarrs::plugin::PluginCreateError;
use zarrs_codec::{
    ArrayCodecTraits, ArrayToBytesCodecTraits, BytesToBytesCodecTraits, Codec, CodecError,
    CodecPluginV3, CodecTraits, CodecTraitsV3,
};

use crate::chunk::{N5ChunkHeader, N5ChunkMode};
use crate::metadata::N5Compression;

// TODO
// ?lz4
// ?xz
// ?blosc
// ?zstd

zarrs::plugin::impl_extension_aliases!(N5Codec, v3: "zarrs.n5", ["zarrs.n5", "n5"]);
inventory::submit! {
    CodecPluginV3::new::<N5Codec>()
}

#[derive(Debug, Clone)]
pub struct N5Codec {
    /// The original representation of the compression.
    n5_compression: N5Compression,
    /// Always contains a big-endian bytes codec.
    /// May contain a single bytes-to-bytes codec representing the N5 compression.
    codecs: CodecChain,
}

impl N5Codec {
    pub fn new(compression: N5Compression) -> crate::Result<Self> {
        let codecs = n5compression_to_chain(&compression)?;
        Ok(Self {
            codecs,
            n5_compression: compression,
        })
    }

    pub fn new_with_configuration(
        configuration: &N5CodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        Self::new(configuration.compression).map_err(|e| PluginCreateError::Other(e.to_string()))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Copy)]
pub struct N5CodecConfiguration {
    compression: N5Compression,
}

fn n5compression_to_b2b(
    n5c: &N5Compression,
) -> crate::Result<Option<Arc<dyn BytesToBytesCodecTraits>>> {
    match n5c {
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

fn n5compression_to_chain(n5c: &N5Compression) -> crate::Result<CodecChain> {
    let compressor = n5compression_to_b2b(n5c)?;
    Ok(CodecChain::new(
        vec![],
        Arc::new(BytesCodec::big()),
        compressor.into_iter().collect(),
    ))
}

impl CodecTraitsV3 for N5Codec {
    fn create(metadata: &MetadataV3) -> Result<zarrs_codec::Codec, zarrs::plugin::PluginCreateError>
    where
        Self: Sized,
    {
        let configuration = metadata.to_typed_configuration()?;
        let codec = Arc::new(N5Codec::new_with_configuration(&configuration)?);
        Ok(Codec::ArrayToBytes(codec))
    }
}

impl CodecTraits for N5Codec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn configuration(
        &self,
        _version: zarrs::plugin::ZarrVersion,
        _options: &zarrs_codec::CodecMetadataOptions,
    ) -> Option<zarrs::metadata::Configuration> {
        let config = N5CodecConfiguration {
            compression: self.n5_compression,
        };
        let val = serde_json::to_value(config).expect("N5 compression should be serializable");
        let serde_json::Value::Object(map) = val else {
            panic!("N5 compression should serialize to a JSON object");
        };
        Some(map.into())
    }

    fn partial_decoder_capability(&self) -> zarrs_codec::PartialDecoderCapability {
        zarrs_codec::PartialDecoderCapability {
            partial_read: false,
            partial_decode: false,
        }
    }

    fn partial_encoder_capability(&self) -> zarrs_codec::PartialEncoderCapability {
        zarrs_codec::PartialEncoderCapability {
            partial_encode: false,
        }
    }
}

impl ArrayCodecTraits for N5Codec {
    fn recommended_concurrency(
        &self,
        _shape: &[std::num::NonZeroU64],
        _data_type: &zarrs::array::DataType,
    ) -> Result<zarrs_codec::RecommendedConcurrency, zarrs_codec::CodecError> {
        Ok(zarrs_codec::RecommendedConcurrency::new_maximum(1))
    }
}

impl ArrayToBytesCodecTraits for N5Codec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits> {
        self
    }

    fn encoded_representation(
        &self,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
        _fill_value: &zarrs::array::FillValue,
    ) -> Result<zarrs_codec::BytesRepresentation, zarrs_codec::CodecError> {
        let ret = if let Some(fs) = data_type.fixed_size() {
            let numel: u64 = shape.iter().map(|n| n.get()).product();
            zarrs_codec::BytesRepresentation::BoundedSize(numel * fs as u64)
        } else {
            zarrs_codec::BytesRepresentation::UnboundedSize
        };
        Ok(ret)
    }

    fn encode<'a>(
        &self,
        _bytes: zarrs_codec::ArrayBytes<'a>,
        _shape: &[std::num::NonZeroU64],
        _data_type: &zarrs::array::DataType,
        _fill_value: &zarrs::array::FillValue,
        _options: &zarrs_codec::CodecOptions,
    ) -> Result<zarrs_codec::ArrayBytesRaw<'a>, zarrs_codec::CodecError> {
        Err(zarrs_codec::CodecError::Other(
            "encoding not supported".into(),
        ))
    }

    fn decode<'a>(
        &self,
        bytes: zarrs_codec::ArrayBytesRaw<'a>,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
        fill_value: &zarrs::array::FillValue,
        options: &zarrs_codec::CodecOptions,
    ) -> Result<zarrs_codec::ArrayBytes<'a>, zarrs_codec::CodecError> {
        let header = N5ChunkHeader::from_bytes(&bytes)
            .map_err(|e| CodecError::Other(format!("N5 chunk header could not be parsed: {e}")))?;

        if !matches!(header.mode, N5ChunkMode::Default) {
            return Err(zarrs_codec::CodecError::Other(format!(
                "unsupported N5 chunk mode: {:?}",
                header.mode
            )));
        }

        // shape should be identical because the regular bounded chunk grid
        // should take care of edge chunks
        let shape_u32: Vec<u32> = shape.iter().map(|n| n.get() as u32).rev().collect();
        if header.shape != shape_u32 {
            return Err(zarrs_codec::CodecError::Other(format!(
                "N5 chunk header has shape {:?}, expected {:?}",
                header.shape, shape,
            )));
        }

        let payload = &bytes[header.data_offset()..];

        self.codecs.decode(
            // TODO: avoid this clone
            Cow::Owned(payload.to_vec()),
            shape,
            data_type,
            fill_value,
            options,
        )
    }
}
