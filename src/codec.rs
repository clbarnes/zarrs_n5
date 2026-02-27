use std::borrow::Cow;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zarrs::array::CodecChain;
use zarrs::array::codec::BytesCodec;
use zarrs::metadata::v3::MetadataV3;
use zarrs::plugin::PluginCreateError;
use zarrs_codec::{
    ArrayCodecTraits, ArrayToBytesCodecTraits, BytesToBytesCodecTraits, Codec, CodecError,
    CodecPluginV3, CodecTraits, CodecTraitsV3,
};

use crate::chunk::{N5ChunkHeader, N5ChunkMode};

// TODO
// ?lz4
// ?xz
// ?blosc
// ?zstd

zarrs::plugin::impl_extension_aliases!(N5Codec, v3: "zarrs.n5", ["zarrs.n5", "n5"]);
inventory::submit! {
    CodecPluginV3::new::<N5Codec>()
}

/// Codec for N5 blocks.
///
/// Validates and strips the header, then applies big-endian byte order and the configured compression codec, if any.
/// Should not be used with any other codecs.
#[derive(Debug, Clone)]
pub struct N5Codec {
    /// Always contains a big-endian bytes codec.
    /// May contain a single bytes-to-bytes codec representing the N5 compression.
    ///
    /// These codecs are only applied to the N5 block body, i.e. not the block header.
    codecs: CodecChain,
}

impl N5Codec {
    pub fn new(compression: Option<Arc<dyn BytesToBytesCodecTraits>>) -> Self {
        let codecs = CodecChain::new(
            vec![],
            Arc::new(BytesCodec::big()),
            compression.into_iter().collect(),
        );
        Self { codecs }
    }

    pub fn new_with_configuration(
        configuration: &N5CodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let Some(cfg) = &configuration.bytes_to_bytes_codec else {
            return Ok(Self::new(None));
        };

        let Codec::BytesToBytes(c) = Codec::from_metadata(cfg)? else {
            return Err(PluginCreateError::Other(format!(
                "metadata does not represent bytes-to-bytes codec: {cfg:?}"
            )));
        };

        Ok(Self::new(Some(c)))
    }

    fn bytes_to_bytes_codec(&self) -> Option<&Arc<dyn BytesToBytesCodecTraits>> {
        self.codecs.bytes_to_bytes_codecs().first()
    }
}

/// Configuration for [N5Codec], which serializes compression configuration, if any.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct N5CodecConfiguration {
    /// Metadata for the bytes-to-bytes codec representing the N5 compression, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bytes_to_bytes_codec: Option<MetadataV3>,
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
        version: zarrs::plugin::ZarrVersion,
        options: &zarrs_codec::CodecMetadataOptions,
    ) -> Option<zarrs::metadata::Configuration> {
        let config = if let Some(c) = self.bytes_to_bytes_codec() {
            let name = c.name(version)?;
            let meta = if let Some(c_cfg) = c.configuration(version, options) {
                MetadataV3::new_with_configuration(name, c_cfg)
            } else {
                MetadataV3::new(name)
            };
            N5CodecConfiguration {
                bytes_to_bytes_codec: Some(meta),
            }
        } else {
            N5CodecConfiguration::default()
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
        let shape_u32: Vec<_> = shape.iter().map(|n| n.get() as u32).rev().collect();
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
