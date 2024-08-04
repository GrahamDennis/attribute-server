use crate::pb;
use crate::pb::watch_entity_rows_event::Event;
use crate::pb::{AttributeValue, EntityRow, NullableAttributeValue, WatchEntityRowsEvent};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use prost_reflect::DynamicMessage;
use serde::ser::{SerializeSeq, SerializeStruct, SerializeStructVariant};
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
                let mut state = serializer.serialize_struct_variant("event", 0, "added", 1)?;

                if let Some(row) = &added_event.entity_row {
                    state.serialize_field("entityRow", &CustomFormat(row, metadata))?;
                } else {
                    state.skip_field("entityRow")?;
                }

                state.end()
            }
            Event::Modified(modified_event) => {
                let mut state = serializer.serialize_struct_variant("event", 1, "modified", 1)?;

                if let Some(row) = &modified_event.entity_row {
                    state.serialize_field("entityRow", &CustomFormat(row, metadata))?;
                } else {
                    state.skip_field("entityRow")?;
                }

                state.end()
            }
            Event::Removed(removed_event) => {
                let mut state = serializer.serialize_struct_variant("event", 2, "added", 1)?;

                if let Some(row) = &removed_event.entity_row {
                    state.serialize_field("entityRow", &CustomFormat(row, metadata))?;
                } else {
                    state.skip_field("entityRow")?;
                }

                state.end()
            }
            Event::Bookmark(bookmark_event) => {
                let mut state = serializer.serialize_struct_variant("event", 3, "bookmark", 1)?;
                state.serialize_field("entityVersion", &bookmark_event.entity_version)?;
                state.end()
            }
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

                state.serialize_element(&dynamic_message)?;
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
