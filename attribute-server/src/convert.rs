use crate::internal_pb;
use crate::pb;
use anyhow::format_err;
use attribute_store::store::{
    AndQueryNode, AttributeToUpdate, AttributeType, AttributeValue, CreateAttributeTypeRequest,
    Entity, EntityId, EntityLocator, EntityQueryNode, EntityRow, EntityRowQuery, EntityVersion,
    HasAttributeTypesNode, MatchAllQueryNode, MatchNoneQueryNode, OrQueryNode, Symbol,
    UpdateEntityRequest, ValueType, WatchEntitiesEvent, WatchEntitiesRequest,
};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use prost::Message;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FieldError {
    #[error("missing field")]
    FieldMissing,
    #[error("error decoding entity id")]
    InvalidEntityId(#[source] anyhow::Error),
    #[error("invalid symbol")]
    InvalidSymbol(#[source] anyhow::Error),
    #[error("invalid value type")]
    InvalidValueType(#[source] anyhow::Error),
}

impl FieldError {
    fn at_path(self, path: garde::Path) -> ConversionError {
        ConversionError::InField(path, self)
    }
}

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("error in field `{0}`")]
    InField(garde::Path, #[source] FieldError),
}

type ConversionResult<T> = Result<T, ConversionError>;

pub trait TryFromProto<T>: Sized {
    fn try_from_proto(value: T) -> ConversionResult<Self> {
        Self::try_from_proto_with(value, &mut garde::Path::empty)
    }

    fn try_from_proto_with(
        value: T,
        parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self>;
}

pub trait IntoProto<T>: Sized {
    fn into_proto(self) -> T;
}

impl TryFromProto<pb::GetEntityRequest> for EntityLocator {
    fn try_from_proto_with(
        value: pb::GetEntityRequest,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "entity_locator");

        let entity_locator = value
            .entity_locator
            .ok_or_else(|| FieldMissing.at_path(path()))?;

        EntityLocator::try_from_proto_with(entity_locator, &mut path)
    }
}

impl TryFromProto<pb::EntityLocator> for EntityLocator {
    fn try_from_proto_with(
        value: pb::EntityLocator,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "locator");

        let locator = value.locator.ok_or_else(|| FieldMissing.at_path(path()))?;

        EntityLocator::try_from_proto_with(locator, &mut path)
    }
}

impl TryFromProto<pb::entity_locator::Locator> for EntityLocator {
    fn try_from_proto_with(
        value: pb::entity_locator::Locator,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use pb::entity_locator::Locator;

        match value {
            Locator::EntityId(entity_id) => {
                let mut path = garde::util::nested_path!(parent, "entity_id");
                EntityId::try_from_proto_with(entity_id, &mut path).map(EntityLocator::EntityId)
            }
            Locator::Symbol(symbol) => {
                let mut path = garde::util::nested_path!(parent, "symbol");
                Symbol::try_from_proto_with(symbol, &mut path).map(EntityLocator::Symbol)
            }
        }
    }
}

impl TryFromProto<String> for EntityId {
    fn try_from_proto_with(
        value: String,
        parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let decoded_bytes = URL_SAFE
            .decode(&value)
            .map_err(|err| InvalidEntityId(err.into()).at_path(parent()))?;
        let internal_entity_id = internal_pb::InternalEntityId::decode(&*decoded_bytes)
            .map_err(|err| InvalidEntityId(err.into()).at_path(parent()))?;
        let entity_id: EntityId = internal_entity_id.database_id.into();

        Ok(entity_id)
    }
}

impl TryFromProto<String> for Symbol {
    fn try_from_proto_with(
        value: String,
        parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        Symbol::try_from(value).map_err(|err| InvalidSymbol(err.into()).at_path(parent()))
    }
}

impl IntoProto<pb::Entity> for Entity {
    fn into_proto(self) -> pb::Entity {
        pb::Entity {
            entity_id: self.entity_id.into_proto(),
            entity_version: self.entity_version.into_proto(),
            attributes: self.attributes.into_proto(),
        }
    }
}

impl IntoProto<String> for EntityId {
    fn into_proto(self) -> String {
        let EntityId(database_id) = self;
        let internal_entity_id = internal_pb::InternalEntityId { database_id };
        URL_SAFE.encode(internal_entity_id.encode_to_vec())
    }
}

impl IntoProto<String> for EntityVersion {
    fn into_proto(self) -> String {
        let EntityVersion(database_id) = self;
        let internal_entity_version = internal_pb::InternalEntityVersion { database_id };
        URL_SAFE.encode(internal_entity_version.encode_to_vec())
    }
}

impl IntoProto<HashMap<String, pb::AttributeValue>> for HashMap<Symbol, AttributeValue> {
    fn into_proto(self) -> HashMap<String, pb::AttributeValue> {
        self.into_iter()
            .map(|(symbol, attribute_value)| (symbol.into(), attribute_value.into_proto()))
            .collect()
    }
}

impl IntoProto<pb::AttributeValue> for AttributeValue {
    fn into_proto(self) -> pb::AttributeValue {
        pb::AttributeValue {
            attribute_value: Some(self.into_proto()),
        }
    }
}

impl IntoProto<pb::attribute_value::AttributeValue> for AttributeValue {
    fn into_proto(self) -> pb::attribute_value::AttributeValue {
        match self {
            AttributeValue::String(string_value) => {
                pb::attribute_value::AttributeValue::StringValue(string_value)
            }
            AttributeValue::EntityId(entity_id) => {
                pb::attribute_value::AttributeValue::EntityIdValue(entity_id.into_proto())
            }
            AttributeValue::Bytes(bytes) => pb::attribute_value::AttributeValue::BytesValue(bytes),
        }
    }
}

impl TryFromProto<pb::QueryEntityRowsRequest> for EntityRowQuery {
    fn try_from_proto_with(
        value: pb::QueryEntityRowsRequest,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        Ok(EntityRowQuery {
            root: {
                let mut path = garde::util::nested_path!(parent, "root");
                let entity_query_node_proto =
                    value.root.ok_or_else(|| FieldMissing.at_path(path()))?;
                EntityQueryNode::try_from_proto_with(entity_query_node_proto, &mut path)?
            },
            attribute_types: {
                let mut path = garde::util::nested_path!(parent, "attribute_types");

                let attribute_types: Result<Vec<Symbol>, _> = value
                    .attribute_types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, attribute_type)| {
                        let mut attribute_type_path = garde::util::nested_path!(path, idx);
                        Symbol::try_from_proto_with(attribute_type, &mut attribute_type_path)
                    })
                    .collect();

                attribute_types?
            },
        })
    }
}

impl TryFromProto<pb::EntityQueryNode> for EntityQueryNode {
    fn try_from_proto_with(
        value: pb::EntityQueryNode,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "query");

        let query_proto = value.query.ok_or_else(|| FieldMissing.at_path(path()))?;
        EntityQueryNode::try_from_proto_with(query_proto, &mut path)
    }
}

impl TryFromProto<pb::entity_query_node::Query> for EntityQueryNode {
    fn try_from_proto_with(
        value: pb::entity_query_node::Query,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use pb::entity_query_node::Query;

        Ok(match value {
            Query::MatchAll(_) => EntityQueryNode::MatchAll(MatchAllQueryNode),
            Query::MatchNone(_) => EntityQueryNode::MatchNone(MatchNoneQueryNode),
            Query::And(and_query_node) => {
                let mut path = garde::util::nested_path!(parent, "and_");
                EntityQueryNode::And(AndQueryNode::try_from_proto_with(
                    and_query_node,
                    &mut path,
                )?)
            }
            Query::Or(or_query_node) => {
                let mut path = garde::util::nested_path!(parent, "or_");
                EntityQueryNode::Or(OrQueryNode::try_from_proto_with(or_query_node, &mut path)?)
            }
            Query::HasAttributeTypes(has_attribute_types_node) => {
                let mut path = garde::util::nested_path!(parent, "has_attribute_types");
                EntityQueryNode::HasAttributeTypes(HasAttributeTypesNode::try_from_proto_with(
                    has_attribute_types_node,
                    &mut path,
                )?)
            }
        })
    }
}

impl TryFromProto<pb::AndQueryNode> for AndQueryNode {
    fn try_from_proto_with(
        value: pb::AndQueryNode,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        let mut path = garde::util::nested_path!(parent, "clauses");
        Ok(AndQueryNode {
            clauses: Vec::try_from_proto_with(value.clauses, &mut path)?,
        })
    }
}

impl TryFromProto<pb::OrQueryNode> for OrQueryNode {
    fn try_from_proto_with(
        value: pb::OrQueryNode,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        let mut path = garde::util::nested_path!(parent, "clauses");
        Ok(OrQueryNode {
            clauses: Vec::try_from_proto_with(value.clauses, &mut path)?,
        })
    }
}

impl TryFromProto<pb::HasAttributeTypesNode> for HasAttributeTypesNode {
    fn try_from_proto_with(
        value: pb::HasAttributeTypesNode,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        let mut path = garde::util::nested_path!(parent, "attribute_types");
        Ok(HasAttributeTypesNode {
            attribute_types: Vec::try_from_proto_with(value.attribute_types, &mut path)?,
        })
    }
}

impl<A, B> TryFromProto<Vec<A>> for Vec<B>
where
    B: TryFromProto<A>,
{
    fn try_from_proto_with(
        value: Vec<A>,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        value
            .into_iter()
            .enumerate()
            .map(|(idx, element)| {
                let mut path = garde::util::nested_path!(parent, idx);
                B::try_from_proto_with(element, &mut path)
            })
            .collect()
    }
}

impl IntoProto<pb::EntityRow> for EntityRow {
    fn into_proto(self) -> pb::EntityRow {
        pb::EntityRow {
            values: self
                .values
                .into_iter()
                .map(|value| pb::NullableAttributeValue {
                    value: value.map(|v| v.into_proto()),
                })
                .collect(),
        }
    }
}

impl TryFromProto<pb::CreateAttributeTypeRequest> for CreateAttributeTypeRequest {
    fn try_from_proto_with(
        value: pb::CreateAttributeTypeRequest,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "attribute_type");

        let attribute_type_proto = value
            .attribute_type
            .ok_or_else(|| FieldMissing.at_path(path()))?;

        Ok(CreateAttributeTypeRequest {
            attribute_type: AttributeType::try_from_proto_with(attribute_type_proto, &mut path)?,
        })
    }
}

impl TryFromProto<pb::AttributeType> for AttributeType {
    fn try_from_proto_with(
        value: pb::AttributeType,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        Ok(AttributeType {
            symbol: {
                let mut path = garde::util::nested_path!(parent, "symbol");
                Symbol::try_from_proto_with(value.symbol, &mut path)?
            },
            value_type: {
                let mut path = garde::util::nested_path!(parent, "value_type");
                let value_type_proto = pb::ValueType::try_from(value.value_type)
                    .map_err(|err| InvalidValueType(err.into()).at_path(path()))?;
                ValueType::try_from_proto_with(value_type_proto, &mut path)?
            },
        })
    }
}

impl TryFromProto<pb::ValueType> for ValueType {
    fn try_from_proto_with(
        value: pb::ValueType,
        parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        match value {
            pb::ValueType::Invalid => {
                Err(InvalidValueType(format_err!("value_type = 0 is not valid")).at_path(parent()))
            }
            pb::ValueType::Text => Ok(ValueType::Text),
            pb::ValueType::EntityReference => Ok(ValueType::EntityReference),
            pb::ValueType::Bytes => Ok(ValueType::Bytes),
        }
    }
}

impl TryFromProto<pb::UpdateEntityRequest> for UpdateEntityRequest {
    fn try_from_proto_with(
        value: pb::UpdateEntityRequest,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        Ok(UpdateEntityRequest {
            entity_locator: {
                let mut path = garde::util::nested_path!(parent, "entity_locator");

                let entity_locator_proto = value
                    .entity_locator
                    .ok_or_else(|| FieldMissing.at_path(path()))?;
                EntityLocator::try_from_proto_with(entity_locator_proto, &mut path)?
            },
            attributes_to_update: {
                let mut path = garde::util::nested_path!(parent, "attributes_to_update");
                let result: Result<Vec<_>, _> = value
                    .attributes_to_update
                    .into_iter()
                    .enumerate()
                    .map(|(idx, attribute_to_update)| {
                        let mut attribute_to_update_path = garde::util::nested_path!(path, idx);
                        AttributeToUpdate::try_from_proto_with(
                            attribute_to_update,
                            &mut attribute_to_update_path,
                        )
                    })
                    .collect();
                result?
            },
        })
    }
}

impl TryFromProto<pb::AttributeToUpdate> for AttributeToUpdate {
    fn try_from_proto_with(
        value: pb::AttributeToUpdate,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        Ok(AttributeToUpdate {
            symbol: {
                let mut path = garde::util::nested_path!(parent, "attribute_type");
                Symbol::try_from_proto_with(value.attribute_type, &mut path)?
            },
            value: {
                let mut path = garde::util::nested_path!(parent, "attribute_value");

                value
                    .attribute_value
                    .map(|proto| AttributeValue::try_from_proto_with(proto, &mut path))
                    .transpose()?
            },
        })
    }
}

impl TryFromProto<pb::NullableAttributeValue> for Option<AttributeValue> {
    fn try_from_proto_with(
        value: pb::NullableAttributeValue,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        value
            .value
            .map(|value| {
                let mut path = garde::util::nested_path!(parent, "value");
                AttributeValue::try_from_proto_with(value, &mut path)
            })
            .transpose()
    }
}

impl TryFromProto<pb::AttributeValue> for AttributeValue {
    fn try_from_proto_with(
        value: pb::AttributeValue,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "attribute_value");

        let attribute_value_proto = value
            .attribute_value
            .ok_or_else(|| FieldMissing.at_path(path()))?;

        AttributeValue::try_from_proto_with(attribute_value_proto, &mut path)
    }
}

impl TryFromProto<pb::attribute_value::AttributeValue> for AttributeValue {
    fn try_from_proto_with(
        value: pb::attribute_value::AttributeValue,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use pb::attribute_value;

        Ok(match value {
            attribute_value::AttributeValue::StringValue(string_value) => {
                AttributeValue::String(string_value)
            }
            attribute_value::AttributeValue::EntityIdValue(external_entity_id) => {
                let mut path = garde::util::nested_path!(parent, "entity_id_value");

                AttributeValue::EntityId(EntityId::try_from_proto_with(
                    external_entity_id,
                    &mut path,
                )?)
            }
            attribute_value::AttributeValue::BytesValue(bytes_value) => {
                AttributeValue::Bytes(bytes_value)
            }
        })
    }
}

impl TryFromProto<pb::WatchEntitiesRequest> for WatchEntitiesRequest {
    fn try_from_proto_with(
        value: pb::WatchEntitiesRequest,
        mut parent: &mut dyn FnMut() -> garde::Path,
    ) -> ConversionResult<Self> {
        use FieldError::*;

        let mut path = garde::util::nested_path!(parent, "query");

        let query_proto = value.query.ok_or_else(|| FieldMissing.at_path(path()))?;
        Ok(WatchEntitiesRequest {
            query: EntityQueryNode::try_from_proto_with(query_proto, &mut path)?,
            send_initial_events: value.send_initial_events,
        })
    }
}

impl IntoProto<pb::WatchEntitiesEvent> for WatchEntitiesEvent {
    fn into_proto(self) -> pb::WatchEntitiesEvent {
        pb::WatchEntitiesEvent {
            event: match (self.before, self.after) {
                (None, Some(after)) => {
                    Some(pb::watch_entities_event::Event::Added(pb::AddedEvent {
                        entity: Some(after.into_proto()),
                    }))
                }
                (Some(_), Some(after)) => Some(pb::watch_entities_event::Event::Modified(
                    pb::ModifiedEvent {
                        entity: Some(after.into_proto()),
                    },
                )),
                (Some(before), None) => {
                    Some(pb::watch_entities_event::Event::Removed(pb::RemovedEvent {
                        entity: Some(before.into_proto()),
                    }))
                }
                (before, after) => {
                    log::warn!("Could not convert watch entities event with before={:?}; after={:?} into protobuf", before, after);
                    None
                }
            },
        }
    }
}
