use prost_reflect::{DynamicMessage, ReflectMessage, SerializeOptions};
use serde::Deserializer;
use serde_path_to_error::Track;
use std::fs::File;
use std::io::BufReader;

pub fn to_json<T: ReflectMessage>(message: &T) -> anyhow::Result<String> {
    let mut buffer = vec![];
    let mut serializer = serde_json::Serializer::new(&mut buffer);
    let mut track = Track::new();
    let wrapped_serializer = serde_path_to_error::Serializer::new(&mut serializer, &mut track);
    let options = SerializeOptions::new().skip_default_fields(false);

    message
        .transcode_to_dynamic()
        .serialize_with_options(wrapped_serializer, &options)
        .map_err(|err| serde_path_to_error::Error::new(track.path(), err))?;

    Ok(String::from_utf8(buffer)?)
}

pub fn parse_from_json_argument<T: ReflectMessage + Default>(
    json_argument: &str,
) -> anyhow::Result<T> {
    let parsed = if let Some(json_file) = json_argument.strip_prefix('@') {
        let mut deserializer =
            serde_json::de::Deserializer::from_reader(BufReader::new(File::open(json_file)?));
        let result = parse_from_deserializer(&mut deserializer)?;
        deserializer.end()?;
        result
    } else {
        let mut deserializer = serde_json::de::Deserializer::from_str(json_argument);
        let result = parse_from_deserializer(&mut deserializer)?;
        deserializer.end()?;
        result
    };

    Ok(parsed)
}

fn parse_from_deserializer<'de, T: ReflectMessage + Default, D: Deserializer<'de>>(
    deserializer: D,
) -> anyhow::Result<T>
where
    <D as Deserializer<'de>>::Error: Send + Sync + 'static,
{
    let mut track = Track::new();
    let wrapped_deserializer = serde_path_to_error::Deserializer::new(deserializer, &mut track);
    let message = DynamicMessage::deserialize(T::default().descriptor(), wrapped_deserializer)
        .map_err(|err| serde_path_to_error::Error::new(track.path(), err))?;

    Ok(message.transcode_to()?)
}
