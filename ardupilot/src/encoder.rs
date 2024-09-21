use bytes::{BufMut, BytesMut};
use mavio::protocol::{MavLinkVersion, MavSTX, MaybeVersioned};
use mavio::{Frame, Sender};
use tokio_util::codec::Encoder;

struct MavlinkEncoder<V: MaybeVersioned> {}

impl<V: MaybeVersioned> Encoder<mavio::Frame<V>> for MavlinkEncoder<V> {
    // FIXME: change error type
    type Error = std::io::Error;

    fn encode(&mut self, frame: Frame<V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut sender = Sender::new(dst.writer());

        match sender.send(&frame) {
            Ok(_) => Ok(()),
            Err(error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        }
    }
}
