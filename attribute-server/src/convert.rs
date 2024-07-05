use crate::convert::ConversionError::InvalidValueType;
use anyhow::anyhow;
use attribute_grpc_api::grpc;
use attribute_grpc_api::grpc::CreateAttributeTypeRequest;
use attribute_store::store::{
    AndQueryNode, AttributeType, AttributeValue, Entity, EntityId, EntityLocator, EntityQuery,
    EntityQueryNode, EntityRow, MatchAllQueryNode, MatchNoneQueryNode, OrQueryNode, Symbol,
    ValueType,
};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use prost::Message;
use std::collections::HashMap;
use thiserror::Error;

pub mod internal {
    tonic::include_proto!("me.grahamdennis.attribute");
}

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("field missing")]
    FieldMissing,
    #[error("error converting field `{field}`")]
    ErrorConvertingField {
        field: &'static str,
        source: Box<ConversionError>,
    },
    #[error("error decoding entity id")]
    InvalidEntityId(#[source] anyhow::Error),
    #[error("invalid symbol")]
    InvalidSymbol(#[source] anyhow::Error),
    #[error("invalid value type")]
    InvalidValueType(#[source] anyhow::Error),
}

type ConversionResult<T> = Result<T, ConversionError>;

pub trait TryFromProto<T>: Sized {
    fn try_from_proto(value: T) -> ConversionResult<Self>;
}

pub trait IntoProto<T>: Sized {
    fn into_proto(self) -> T;
}

impl TryFromProto<grpc::GetEntityRequest> for EntityLocator {
    fn try_from_proto(value: grpc::GetEntityRequest) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .entity_locator
            .ok_or(FieldMissing)
            .and_then(|loc| EntityLocator::try_from_proto(loc))
            .map_err(|err| ErrorConvertingField {
                field: "entity_locator",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::EntityLocator> for EntityLocator {
    fn try_from_proto(value: grpc::EntityLocator) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .locator
            .ok_or(FieldMissing)
            .and_then(|loc| EntityLocator::try_from_proto(loc))
            .map_err(|err| ErrorConvertingField {
                field: "locator",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::entity_locator::Locator> for EntityLocator {
    fn try_from_proto(value: grpc::entity_locator::Locator) -> ConversionResult<Self> {
        use grpc::entity_locator::Locator;

        match value {
            Locator::EntityId(entity_id) => {
                EntityId::try_from_proto(entity_id).map(EntityLocator::EntityId)
            }
            Locator::Symbol(symbol) => Symbol::try_from_proto(symbol).map(EntityLocator::Symbol),
        }
    }
}

impl TryFromProto<String> for EntityId {
    fn try_from_proto(value: String) -> ConversionResult<Self> {
        use ConversionError::*;

        let decoded_bytes = URL_SAFE
            .decode(&value)
            .map_err(|err| InvalidEntityId(err.into()))?;
        let internal_entity_id = internal::InternalEntityId::decode(&*decoded_bytes)
            .map_err(|err| InvalidEntityId(err.into()))?;
        let entity_id: EntityId = internal_entity_id.database_id.into();
        Ok(entity_id)
    }
}

impl TryFromProto<String> for Symbol {
    fn try_from_proto(value: String) -> ConversionResult<Self> {
        use ConversionError::*;

        Symbol::try_from(value).map_err(|err| InvalidSymbol(err.into()))
    }
}

impl IntoProto<grpc::Entity> for Entity {
    fn into_proto(self) -> grpc::Entity {
        grpc::Entity {
            entity_id: self.entity_id.into_proto(),
            attributes: self.attributes.into_proto(),
        }
    }
}

impl IntoProto<String> for EntityId {
    fn into_proto(self) -> String {
        let EntityId(database_id) = self;
        let internal_entity_id = internal::InternalEntityId { database_id };
        URL_SAFE.encode(internal_entity_id.encode_to_vec())
    }
}

impl IntoProto<HashMap<String, grpc::AttributeValue>> for HashMap<Symbol, AttributeValue> {
    fn into_proto(self) -> HashMap<String, grpc::AttributeValue> {
        self.into_iter()
            .map(|(symbol, attribute_value)| (symbol.into(), attribute_value.into_proto()))
            .collect()
    }
}

impl IntoProto<grpc::AttributeValue> for AttributeValue {
    fn into_proto(self) -> grpc::AttributeValue {
        grpc::AttributeValue {
            attribute_value: Some(self.into_proto()),
        }
    }
}

impl IntoProto<grpc::attribute_value::AttributeValue> for AttributeValue {
    fn into_proto(self) -> grpc::attribute_value::AttributeValue {
        match self {
            AttributeValue::String(string_value) => {
                grpc::attribute_value::AttributeValue::StringValue(string_value)
            }
            AttributeValue::EntityId(entity_id) => {
                grpc::attribute_value::AttributeValue::EntityIdValue(entity_id.into_proto())
            }
            AttributeValue::Bytes(bytes) => {
                grpc::attribute_value::AttributeValue::BytesValue(bytes)
            }
        }
    }
}

impl TryFromProto<grpc::QueryEntitiesRequest> for EntityQuery {
    fn try_from_proto(value: grpc::QueryEntitiesRequest) -> ConversionResult<Self> {
        use ConversionError::*;

        let attribute_types: Result<Vec<Symbol>, _> = value
            .attribute_types
            .into_iter()
            .map(|attribute_type| Symbol::try_from_proto(attribute_type))
            .collect();

        Ok(EntityQuery {
            root: value
                .root
                .ok_or(FieldMissing)
                .and_then(|entity_query_node| EntityQueryNode::try_from_proto(entity_query_node))
                .map_err(|err| ErrorConvertingField {
                    field: "root",
                    source: err.into(),
                })?,
            attribute_types: attribute_types.map_err(|err| ErrorConvertingField {
                field: "attribute_types",
                source: err.into(),
            })?,
        })
    }
}

impl TryFromProto<grpc::EntityQueryNode> for EntityQueryNode {
    fn try_from_proto(value: grpc::EntityQueryNode) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .query
            .ok_or(FieldMissing)
            .and_then(|node| EntityQueryNode::try_from_proto(node))
            .map_err(|err| ErrorConvertingField {
                field: "query",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::entity_query_node::Query> for EntityQueryNode {
    fn try_from_proto(value: grpc::entity_query_node::Query) -> ConversionResult<Self> {
        use grpc::entity_query_node::Query;

        Ok(match value {
            Query::MatchAll(_) => EntityQueryNode::MatchAll(MatchAllQueryNode),
            Query::MatchNone(_) => EntityQueryNode::MatchNone(MatchNoneQueryNode),
            Query::And(and_query_node) => {
                EntityQueryNode::And(AndQueryNode::try_from_proto(and_query_node)?)
            }
            Query::Or(or_query_node) => {
                EntityQueryNode::Or(OrQueryNode::try_from_proto(or_query_node)?)
            }
        })
    }
}

impl TryFromProto<grpc::AndQueryNode> for AndQueryNode {
    fn try_from_proto(value: grpc::AndQueryNode) -> ConversionResult<Self> {
        use ConversionError::*;

        Ok(AndQueryNode {
            clauses: Vec::try_from_proto(value.clauses).map_err(|err| ErrorConvertingField {
                field: "clauses",
                source: err.into(),
            })?,
        })
    }
}

impl TryFromProto<grpc::OrQueryNode> for OrQueryNode {
    fn try_from_proto(value: grpc::OrQueryNode) -> ConversionResult<Self> {
        use ConversionError::*;

        Ok(OrQueryNode {
            clauses: Vec::try_from_proto(value.clauses).map_err(|err| ErrorConvertingField {
                field: "clauses",
                source: err.into(),
            })?,
        })
    }
}

impl TryFromProto<Vec<grpc::EntityQueryNode>> for Vec<EntityQueryNode> {
    fn try_from_proto(value: Vec<grpc::EntityQueryNode>) -> ConversionResult<Self> {
        value
            .into_iter()
            .map(EntityQueryNode::try_from_proto)
            .collect()
    }
}

impl IntoProto<grpc::EntityRow> for EntityRow {
    fn into_proto(self) -> grpc::EntityRow {
        grpc::EntityRow {
            values: self
                .values
                .into_iter()
                .map(|value| grpc::NullableAttributeValue {
                    value: value.map(|v| v.into_proto()),
                })
                .collect(),
        }
    }
}

impl TryFromProto<grpc::CreateAttributeTypeRequest> for AttributeType {
    fn try_from_proto(value: CreateAttributeTypeRequest) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .attribute_type
            .ok_or(FieldMissing)
            .and_then(AttributeType::try_from_proto)
            .map_err(|err| ErrorConvertingField {
                field: "attribute_type",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::AttributeType> for AttributeType {
    fn try_from_proto(value: grpc::AttributeType) -> ConversionResult<Self> {
        use ConversionError::*;

        let value_type = grpc::ValueType::try_from(value.value_type)
            .map_err(|err| InvalidValueType(err.into()))
            .and_then(ValueType::try_from_proto)
            .map_err(|err| ErrorConvertingField {
                field: "value_type",
                source: err.into(),
            })?;

        Ok(AttributeType {
            symbol: Symbol::try_from_proto(value.symbol).map_err(|err| ErrorConvertingField {
                field: "symbol",
                source: err.into(),
            })?,
            value_type,
        })
    }
}

impl TryFromProto<grpc::ValueType> for ValueType {
    fn try_from_proto(value: grpc::ValueType) -> ConversionResult<Self> {
        match value {
            grpc::ValueType::Invalid => {
                Err(InvalidValueType(anyhow!("value_type = 0 is not valid")))
            }
            grpc::ValueType::Text => Ok(ValueType::Text),
            grpc::ValueType::EntityReference => Ok(ValueType::EntityReference),
            grpc::ValueType::Bytes => Ok(ValueType::Bytes),
        }
    }
}
