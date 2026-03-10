use std::borrow::Cow;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zarrs::array::codec::api::{
    ArrayBytes, ArrayBytesRaw, ArrayCodecTraits, ArrayToBytesCodecTraits, BytesRepresentation,
    BytesToBytesCodecTraits, Codec, CodecError, CodecMetadataOptions, CodecOptions, CodecPluginV3,
    CodecTraits, CodecTraitsV3, PartialDecoderCapability, PartialEncoderCapability,
    RecommendedConcurrency,
};
use zarrs::array::codec::{BytesCodec, TransposeOrder};
use zarrs::array::{CodecChain, codec::TransposeCodec};
use zarrs::metadata::v3::MetadataV3;
use zarrs::plugin::PluginCreateError;

use crate::chunk::{N5BlockHeader, N5BlockMode};

// TODO
// ?lz4
// ?xz

zarrs::plugin::impl_extension_aliases!(N5DefaultCodec, v3: "n5_default", ["zarrs.n5_default"]);
inventory::submit! {
    CodecPluginV3::new::<N5DefaultCodec>()
}

/// Codec for N5 blocks.
///
/// Validates and strips the header, then applies big-endian byte order and the configured compression codec, if any.
/// Should not be used with any other codecs.
#[derive(Debug, Clone)]
pub struct N5DefaultCodec {
    /// Always contains a big-endian bytes codec.
    /// May contain a single bytes-to-bytes codec representing the N5 compression.
    ///
    /// These codecs are only applied to the N5 block body, i.e. not the block header.
    codecs: CodecChain,
}

impl N5DefaultCodec {
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
        configuration: &N5DefaultCodecConfiguration,
    ) -> Result<Self, PluginCreateError> {
        let codecs = CodecChain::from_metadata(&configuration.codecs)?;
        Ok(Self { codecs })
    }
}

/// Configuration for [N5Codec], which serializes compression configuration, if any.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct N5DefaultCodecConfiguration {
    /// Codecs to apply to the block body, i.e. after stripping the N5 block header.
    codecs: Vec<MetadataV3>,
}

impl CodecTraitsV3 for N5DefaultCodec {
    fn create(metadata: &MetadataV3) -> Result<Codec, zarrs::plugin::PluginCreateError>
    where
        Self: Sized,
    {
        let configuration = metadata.to_typed_configuration()?;
        let codec = Arc::new(N5DefaultCodec::new_with_configuration(&configuration)?);
        Ok(Codec::ArrayToBytes(codec))
    }
}

impl CodecTraits for N5DefaultCodec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn configuration(
        &self,
        _version: zarrs::plugin::ZarrVersion,
        options: &CodecMetadataOptions,
    ) -> Option<zarrs::metadata::Configuration> {
        let metadatas = self.codecs.create_metadatas(options);
        let config = N5DefaultCodecConfiguration { codecs: metadatas };
        let val = serde_json::to_value(config).expect("N5 compression should be serializable");
        let serde_json::Value::Object(map) = val else {
            panic!("N5 compression should serialize to a JSON object");
        };
        Some(map.into())
    }

    fn partial_decoder_capability(&self) -> PartialDecoderCapability {
        PartialDecoderCapability {
            partial_read: false,
            partial_decode: false,
        }
    }

    fn partial_encoder_capability(&self) -> PartialEncoderCapability {
        PartialEncoderCapability {
            partial_encode: false,
        }
    }
}

impl ArrayCodecTraits for N5DefaultCodec {
    fn recommended_concurrency(
        &self,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
    ) -> Result<RecommendedConcurrency, CodecError> {
        self.codecs.recommended_concurrency(shape, data_type)
    }
}

impl ArrayToBytesCodecTraits for N5DefaultCodec {
    fn into_dyn(self: Arc<Self>) -> Arc<dyn ArrayToBytesCodecTraits> {
        self
    }

    fn encoded_representation(
        &self,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
        fill_value: &zarrs::array::FillValue,
    ) -> Result<BytesRepresentation, CodecError> {
        self.codecs
            .encoded_representation(shape, data_type, fill_value)
    }

    fn encode<'a>(
        &self,
        _bytes: ArrayBytes<'a>,
        _shape: &[std::num::NonZeroU64],
        _data_type: &zarrs::array::DataType,
        _fill_value: &zarrs::array::FillValue,
        _options: &CodecOptions,
    ) -> Result<ArrayBytesRaw<'a>, CodecError> {
        Err(CodecError::Other("encoding not supported".into()))
    }

    fn decode<'a>(
        &self,
        bytes: ArrayBytesRaw<'a>,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
        fill_value: &zarrs::array::FillValue,
        options: &CodecOptions,
    ) -> Result<ArrayBytes<'a>, CodecError> {
        let header = N5BlockHeader::from_bytes(&bytes)
            .map_err(|e| CodecError::Other(format!("N5 block header could not be parsed: {e}")))?;

        if !matches!(header.mode, N5BlockMode::Default) {
            return Err(CodecError::Other(format!(
                "unsupported N5 block mode: {:?}",
                header.mode
            )));
        }

        let header_shape: Vec<_> = header
            .shape
            .iter()
            .map(|n| std::num::NonZeroU64::new(*n as u64).unwrap())
            .collect();

        let payload = &bytes[header.data_offset()..];

        let array_bytes = self.codecs.decode(
            Cow::Borrowed(payload),
            &header_shape,
            data_type,
            fill_value,
            options,
        )?;

        super::ShapeRectifier::new_unchecked(
            array_bytes,
            &header_shape,
            data_type,
            fill_value,
            shape,
        )
        .rectify()
    }
}
