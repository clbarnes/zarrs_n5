use std::borrow::Cow;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zarrs::array::codec::{BytesCodec, TransposeOrder};
use zarrs::array::{CodecChain, codec::TransposeCodec};
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
    pub fn new(compression: Option<Arc<dyn BytesToBytesCodecTraits>>, ndim: usize) -> Self {
        let transpose_order = (0..ndim).rev().collect::<Vec<_>>();
        let codecs = CodecChain::new(
            vec![Arc::new(TransposeCodec::new(
                TransposeOrder::new(&transpose_order).unwrap(),
            ))],
            Arc::new(BytesCodec::big()),
            compression.into_iter().collect(),
        );
        Self { codecs }
    }

    pub fn new_with_configuration(
        configuration: &N5CodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let codecs = CodecChain::from_metadata(&configuration.codecs)?;
        Ok(Self { codecs })
    }
}

/// Configuration for [N5Codec], which serializes compression configuration, if any.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct N5CodecConfiguration {
    /// Codecs to apply to the block body, i.e. after stripping the N5 block header.
    codecs: Vec<MetadataV3>,
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
        options: &zarrs_codec::CodecMetadataOptions,
    ) -> Option<zarrs::metadata::Configuration> {
        let metadatas = self.codecs.create_metadatas(options);
        let config = N5CodecConfiguration { codecs: metadatas };
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
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
    ) -> Result<zarrs_codec::RecommendedConcurrency, zarrs_codec::CodecError> {
        self.codecs.recommended_concurrency(shape, data_type)
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
        fill_value: &zarrs::array::FillValue,
    ) -> Result<zarrs_codec::BytesRepresentation, zarrs_codec::CodecError> {
        self.codecs
            .encoded_representation(shape, data_type, fill_value)
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
            .map_err(|e| CodecError::Other(format!("N5 block header could not be parsed: {e}")))?;

        if !matches!(header.mode, N5ChunkMode::Default) {
            return Err(zarrs_codec::CodecError::Other(format!(
                "unsupported N5 block mode: {:?}",
                header.mode
            )));
        }

        // shape should be identical because the regular bounded chunk grid
        // should take care of edge chunks
        let shape_u32: Vec<_> = shape.iter().map(|n| n.get() as u32).collect();
        if header.shape != shape_u32 {
            return Err(zarrs_codec::CodecError::Other(format!(
                "N5 block header has shape {:?}, expected {:?}",
                header.shape, shape_u32,
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
