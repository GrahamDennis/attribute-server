use crate::pb;
use crate::pb::watch_entity_rows_event::Event;
use crate::pb::{AttributeValue, EntityRow, NullableAttributeValue, WatchEntityRowsEvent};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use prost_reflect::{DynamicMessage, SerializeOptions};
use serde::ser::{SerializeSeq, SerializeStruct};
use serde::{ser, Serialize, Serializer};
use std::fmt::Debug;
use std::iter;

#[derive(Debug, Clone)]
pub enum ColumnMetadata {
    MessageDescriptor(prost_reflect::MessageDescriptor),
}

#[derive(Debug, Clone)]
pub struct EntityRowMetadata {
    pub columns: Vec<Option<ColumnMetadata>>,
}

pub fn wrap_watch_entity_rows_event<'a>(
    event: &'a WatchEntityRowsEvent,
    metadata: &'a EntityRowMetadata,
) -> impl Serialize + 'a {
    CustomFormat(event, metadata)
}

struct WithSerializeOptions<'a, T>(T, &'a SerializeOptions);

impl<'a> Serialize for WithSerializeOptions<'a, DynamicMessage> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let WithSerializeOptions(message, serialize_options) = self;
        message.serialize_with_options(serializer, serialize_options)
    }
}

struct CustomFormat<'a, T>(T, &'a EntityRowMetadata);

impl<'a> Serialize for CustomFormat<'a, &WatchEntityRowsEvent> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let CustomFormat(watch_entity_rows_event, metadata) = self;
        let mut state = serializer.serialize_struct("WatchEntityRowsEvent", 1)?;

        if let Some(event) = &watch_entity_rows_event.event {
            state.serialize_field("event", &CustomFormat(event, metadata))?;
        } else {
            state.skip_field("event")?;
        }

        state.end()
    }
}

impl<'a> Serialize for CustomFormat<'a, &Event> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let CustomFormat(event, metadata) = self;
        match event {
            Event::Added(added_event) => {
                if let Some(row) = &added_event.entity_row {
                    serializer.serialize_newtype_variant(
                        "event",
                        0,
                        "added",
                        &CustomFormat(row, metadata),
                    )
                } else {
                    serializer.serialize_unit()
                }
            }
            Event::Modified(modified_event) => {
                if let Some(row) = &modified_event.entity_row {
                    serializer.serialize_newtype_variant(
                        "event",
                        1,
                        "modified",
                        &CustomFormat(row, metadata),
                    )
                } else {
                    serializer.serialize_unit()
                }
            }
            Event::Removed(removed_event) => {
                if let Some(row) = &removed_event.entity_row {
                    serializer.serialize_newtype_variant(
                        "event",
                        2,
                        "removed",
                        &CustomFormat(row, metadata),
                    )
                } else {
                    serializer.serialize_unit()
                }
            }
            Event::Bookmark(bookmark_event) => serializer.serialize_newtype_variant(
                "event",
                3,
                "bookmark",
                &bookmark_event.entity_version,
            ),
        }
    }
}

impl<'a> Serialize for CustomFormat<'a, &EntityRow> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let CustomFormat(entity_row, metadata) = self;

        let mut state = serializer.serialize_seq(Some(entity_row.values.len()))?;

        for (entry, column) in iter::zip(&entity_row.values, &metadata.columns) {
            if let (
                NullableAttributeValue {
                    value:
                        Some(AttributeValue {
                            attribute_value:
                                Some(pb::attribute_value::AttributeValue::BytesValue(bytes)),
                        }),
                },
                Some(ColumnMetadata::MessageDescriptor(message_descriptor)),
            ) = (entry, column)
            {
                let dynamic_message =
                    DynamicMessage::decode(message_descriptor.clone(), bytes.as_slice()).map_err(
                        |err| {
                            ser::Error::custom(format!(
                                "Failed to decode bytes {bytes:?} for column {column:?}: {err}"
                            ))
                        },
                    )?;
                let serialize_options = SerializeOptions::new().skip_default_fields(false);

                state.serialize_element(&WithSerializeOptions(
                    dynamic_message,
                    &serialize_options,
                ))?;
                continue;
            }

            let attribute_value = entry
                .value
                .as_ref()
                .and_then(|attribute_value| attribute_value.attribute_value.as_ref());

            match attribute_value {
                None => {
                    state.serialize_element(&None::<String>)?;
                }
                Some(pb::attribute_value::AttributeValue::StringValue(s)) => {
                    state.serialize_element(&s)?;
                }
                Some(pb::attribute_value::AttributeValue::EntityIdValue(entity_id)) => {
                    state.serialize_element(&entity_id)?;
                }
                Some(pb::attribute_value::AttributeValue::BytesValue(bytes)) => {
                    state.serialize_element(&STANDARD.encode(&bytes))?;
                }
            }
        }

        state.end()
    }
}
