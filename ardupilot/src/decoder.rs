use std::io::Cursor;
use std::marker::PhantomData;
use tokio_util::codec::Decoder;
use bytes::{BytesMut, Buf};
use mavio::consts::{HEADER_MAX_SIZE, HEADER_V1_SIZE, HEADER_V2_SIZE, SIGNATURE_LENGTH};
use mavio::{protocol, Frame};
use mavio::protocol::{Checksum, CompatFlags, Header, IncompatFlags, MavLinkVersion, MavSTX, MessageId, Payload, Signature};
use protocol::MaybeVersioned;
use tokio::io::AsyncReadExt;

struct MavlinkDecoder<V: MaybeVersioned> {}

const MAX: usize = 8 * 1024 * 1024;

fn find_frame_start<V: MaybeVersioned>(src: &BytesMut) -> Option<(usize, MavLinkVersion)> {
    for (idx, &byte) in src.iter().enumerate() {
        if V::is_magic_byte(byte) {
            return Some((idx, MavSTX::from(byte).into()));
        }
    }

    None
}

impl <V: MaybeVersioned> Decoder for MavlinkDecoder<V> {
    type Item = mavio::Frame<V>;
    // FIXME: change error type
    type Error = std::io::Error;


    fn decode(
        &mut self,
        src: &mut BytesMut
    ) -> Result<Option<Self::Item>, Self::Error> {
        let Some((frame_start, mavlink_version)) = find_frame_start::<V>(src) else {
            return Ok(None);
        };
        if frame_start > 0 {
            src.advance(frame_start);
        }
        let header_size = match mavlink_version {
            MavLinkVersion::V1 => HEADER_V1_SIZE,
            MavLinkVersion::V2 => HEADER_V2_SIZE,
        };

        if src.len() < header_size {
            // Not enough data to read the header
            return Ok(None);
        }

        let mut cursor = Cursor::new(&*src);
        cursor.advance(1); // mavlink version
        let payload_length: u8 = cursor.get_u8();
        if src.len() < header_size + payload_length as usize {
            // Not enough data to read payload
            return Ok(None);
        }

        let (incompat_flags, compat_flags) = if let MavLinkVersion::V2 = mavlink_version {
            let incompat_flags = cursor.get_u8();
            let compat_flags = cursor.get_u8();
            (
                IncompatFlags::from_bits_truncate(incompat_flags),
                CompatFlags::from_bits_truncate(compat_flags),
            )
        } else {
            (IncompatFlags::default(), CompatFlags::default())
        };

        let sequence: u8 = cursor.get_u8();
        let system_id: u8 = cursor.get_u8();
        let component_id: u8 = cursor.get_u8();

        let message_id: MessageId = match mavlink_version {
            MavLinkVersion::V1 => {
                cursor.get_u8() as MessageId
            }
            MavLinkVersion::V2 => {
                let message_id_bytes: [u8; 4] = [
                    cursor.get_u8(),
                    cursor.get_u8(),
                    cursor.get_u8(),
                    0,
                ];
                MessageId::from_le_bytes(message_id_bytes)
            }
        };

        let header = Header::builder()
            .version::<V>(mavlink_version)
            .payload_length(payload_length)
            .incompat_flags(incompat_flags)
            .compat_flags(compat_flags)
            .sequence(sequence)
            .system_id(system_id)
            .component_id(component_id)
            .message_id(message_id)
            .build();

        let body_length = header.body_length();
        if src.len() < header_size + body_length {
            return Ok(None);
        }

        let Some(payload_bytes): Option<&[u8]> = src.get(cursor.position()..(cursor.position() + payload_length as u64)) else {
            return Ok(None);
        };
        cursor.advance(payload_length as usize);
        let payload = Payload::new(header.message_id(), payload_bytes, header.version());

        let checksum: Checksum = cursor.get_u16_le();

        let signature: Option<Signature> = if header.is_signed() {

            let mut signature_bytes: [u8; SIGNATURE_LENGTH] = [0; SIGNATURE_LENGTH];
            cursor.copy_to_slice(&mut signature_bytes);
            Some(Signature::from_byte_array(signature_bytes))
        } else { None };

        Ok(Some(Frame::builder()
                    .version::<V>(mavlink_version)
                    .sequence(sequence)
                    .system_id(system_id)
                    .component_id(component_id)
                    .message_id(message_id)
                    .crc_extra(crc_extra)


                {
            header, payload, checksum, signature
        }))
    }
}
