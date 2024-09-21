use bytes::{Buf, BytesMut};
use mavio::protocol::{MavLinkVersion, MavSTX, MaybeVersioned};
use mavio::Receiver;
use std::io::Cursor;
use tokio_util::codec::Decoder;

struct MavlinkDecoder<V: MaybeVersioned> {}

fn find_frame_start<V: MaybeVersioned>(src: &BytesMut) -> Option<(usize, MavLinkVersion)> {
    for (idx, &byte) in src.iter().enumerate() {
        if V::is_magic_byte(byte) {
            return Some((idx, MavSTX::from(byte).into()));
        }
    }

    None
}

impl<V: MaybeVersioned> Decoder for MavlinkDecoder<V> {
    type Item = mavio::Frame<V>;
    // FIXME: change error type
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some((frame_start, _mavlink_version)) = find_frame_start::<V>(src) else {
            return Ok(None);
        };
        if frame_start > 0 {
            src.advance(frame_start);
        }
        let cursor = Cursor::new(&*src);
        let mut receiver = Receiver::new::<V>(cursor);
        match receiver.recv() {
            Ok(frame) => Ok(Some(frame)),
            Err(mavio::error::Error::Io(io_error))
                if io_error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                Ok(None)
            }
            Err(error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        }
    }
}
