use std::borrow::Cow;
use std::num::NonZeroU64;
use std::ops::{Deref, Range};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zarrs::array::codec::api::{
    ArrayBytes, ArrayBytesOffsets, ArrayBytesRaw, ArrayBytesVariableLength, ArrayCodecTraits,
    ArrayToBytesCodecTraits, BytesRepresentation, BytesToBytesCodecTraits, Codec, CodecError,
    CodecMetadataOptions, CodecOptions, CodecPluginV3, CodecTraits, CodecTraitsV3,
    PartialDecoderCapability, PartialEncoderCapability, RecommendedConcurrency,
};
use zarrs::array::codec::{BytesCodec, TransposeOrder};
use zarrs::array::{CodecChain, codec::TransposeCodec};
use zarrs::array::{DataType, FillValue};
use zarrs::metadata::v3::MetadataV3;
use zarrs::plugin::PluginCreateError;

use crate::chunk::{N5BlockHeader, N5BlockMode};

// TODO
// ?lz4
// ?xz

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
    fn create(metadata: &MetadataV3) -> Result<Codec, zarrs::plugin::PluginCreateError>
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
        options: &CodecMetadataOptions,
    ) -> Option<zarrs::metadata::Configuration> {
        let metadatas = self.codecs.create_metadatas(options);
        let config = N5CodecConfiguration { codecs: metadatas };
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

impl ArrayCodecTraits for N5Codec {
    fn recommended_concurrency(
        &self,
        shape: &[std::num::NonZeroU64],
        data_type: &zarrs::array::DataType,
    ) -> Result<RecommendedConcurrency, CodecError> {
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

        ShapeRectifier::new_unchecked(array_bytes, &header_shape, data_type, fill_value, shape)
            .rectify()
    }
}

struct ShapeRectifier<'a> {
    array_bytes: ArrayBytes<'a>,
    shape: &'a [NonZeroU64],
    data_type: &'a DataType,
    fill_value: &'a FillValue,
    desired_shape: &'a [NonZeroU64],
}

impl<'a> ShapeRectifier<'a> {
    fn new_unchecked(
        array_bytes: ArrayBytes<'a>,
        shape: &'a [NonZeroU64],
        data_type: &'a DataType,
        fill_value: &'a FillValue,
        desired_shape: &'a [NonZeroU64],
    ) -> Self {
        Self {
            array_bytes,
            shape,
            data_type,
            fill_value,
            desired_shape,
        }
    }

    fn rectify(self) -> Result<ArrayBytes<'static>, CodecError> {
        if self.shape == self.desired_shape {
            return Ok(self.array_bytes.into_owned());
        }
        match &self.array_bytes {
            ArrayBytes::Fixed(cow) => {
                let Some(width) = self.data_type.fixed_size() else {
                    return Err(CodecError::Other(format!(
                        "array bytes have a fixed width but {} does not",
                        self.data_type
                    )));
                };
                self.handle_fixed(cow, width)
            }
            ArrayBytes::Variable(abvl) => {
                let offsets = abvl.offsets().deref();
                let bytes = abvl.bytes().deref();
                self.handle_variable(bytes, offsets)
            }
            ArrayBytes::Optional(_) => {
                unreachable!("optional data should not be present in N5 blocks")
            }
        }
    }

    fn handle_fixed(
        &self,
        bytes: &[u8],
        type_width: usize,
    ) -> Result<ArrayBytes<'static>, CodecError> {
        let desired_numel: usize = self
            .desired_shape
            .iter()
            .map(|n| n.get() as usize)
            .product();
        let mut out = Vec::with_capacity(desired_numel * type_width);
        let mut out_idx_iter = IdxIter::new(self.desired_shape);
        let raveller = Raveller::new(self.shape);
        while out_idx_iter.incr() {
            let current = out_idx_iter.current().expect("just checked incr");
            let Some(idx) = raveller.linearize(current) else {
                out.extend_from_slice(self.fill_value.as_ne_bytes());
                continue;
            };
            let start = idx * type_width;
            let end = start + type_width;
            out.extend_from_slice(&bytes[start..end]);
        }
        Ok(ArrayBytes::Fixed(Cow::Owned(out)))
    }

    fn handle_variable(
        &self,
        bytes: &[u8],
        offsets: &[usize],
    ) -> Result<ArrayBytes<'static>, CodecError> {
        let desired_numel: usize = self
            .desired_shape
            .iter()
            .map(|n| n.get() as usize)
            .product();
        let mut out_offsets = Vec::with_capacity(desired_numel + 1);
        out_offsets.push(0usize);

        let mut out = Vec::new();
        let mut out_idx_iter = IdxIter::new(self.desired_shape);
        let raveller = Raveller::new(self.shape);
        while out_idx_iter.incr() {
            let current = out_idx_iter.current().expect("just checked incr");
            let Some(idx) = raveller.linearize(current) else {
                out.extend_from_slice(self.fill_value.as_ne_bytes());
                out_offsets.push(out.len());

                continue;
            };

            let start = offsets[idx];
            let end = offsets[idx + 1];
            out.extend_from_slice(&bytes[start..end]);
            out_offsets.push(out.len());
        }
        Ok(ArrayBytes::Variable(ArrayBytesVariableLength::new(
            out,
            ArrayBytesOffsets::new(out_offsets)?,
        )?))
    }
}

struct IdxIter<'a> {
    max_shape: &'a [NonZeroU64],
    current: Vec<usize>,
    started: bool,
    finished: bool,
}

impl<'a> IdxIter<'a> {
    pub fn new(max_shape: &'a [NonZeroU64]) -> Self {
        let current = vec![0; max_shape.len()];
        Self {
            max_shape,
            current,
            started: false,
            finished: false,
        }
    }

    /// Returns whether there is a new item to show.
    pub fn incr(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            self.started = true;
            return true;
        }

        for idx in (0..self.current.len()).rev() {
            self.current[idx] += 1;
            if (self.current[idx] as u64) < self.max_shape[idx].get() {
                break;
            } else if idx == 0 {
                self.finished = true;
                return false;
            } else {
                self.current[idx] = 0;
            }
        }
        true
    }

    /// Get the current value
    pub fn current(&self) -> Option<&[usize]> {
        if !self.started || self.finished {
            None
        } else {
            Some(&self.current)
        }
    }
}

struct Raveller<'a> {
    shape: &'a [NonZeroU64],
}

impl<'a> Raveller<'a> {
    pub fn new(shape: &'a [NonZeroU64]) -> Self {
        Self { shape }
    }
}

impl<'a> Raveller<'a> {
    /// Panics if `idx.len() != self.shape.len()`
    fn linearize(&self, idx: &[usize]) -> Option<usize> {
        assert!(idx.len() == self.shape.len());
        let mut out = 0;
        let mut stride = 1;
        for (i, s) in idx.iter().zip(self.shape.iter()).rev() {
            if *i as u64 >= s.get() {
                return None;
            }
            out += i * stride;
            stride *= s.get() as usize;
        }
        Some(out)
    }

    #[allow(unused)]
    /// Get the linear indices of the given row.
    /// The row is specified by all but the last dimension (i.e. C contiguous).
    ///
    /// Panics if `idx.len() != self.shape.len() - 1`.
    fn linearize_row(&self, idx: &[usize]) -> Option<Range<usize>> {
        assert!(idx.len() == self.shape.len() - 1);
        let mut start = 0;
        let end = self.shape.len() - 1;
        let last_shape = self.shape[end].get() as usize;
        let mut stride = last_shape;
        for (i, s) in idx.iter().zip(self.shape[..end].iter()).rev() {
            if *i as u64 >= s.get() {
                return None;
            }
            start += i * stride;
            stride *= s.get() as usize;
        }
        Some(start..start + last_shape)
    }

    #[allow(unused)]
    fn row_len(&self) -> usize {
        self.shape.last().unwrap().get() as usize
    }
}
