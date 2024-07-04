use async_trait::async_trait;
use regex::Regex;
use std::collections::HashMap;
use std::convert::Into;
use std::string::ToString;
use std::sync::OnceLock;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AttributeStoreError {
    #[error("name `{0}` is not a valid symbol name")]
    InvalidSymbolName(String),
    #[error("internal error: `{0}`")]
    InternalError(&'static str),
    #[error("entity not found (locator: `{0:?}`)")]
    EntityNotFound(EntityLocator),
    #[error("unregistered attribute type/s: `{0:?}`")]
    UnregisteredAttributeTypes(Vec<Symbol>),
}

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone)]
pub struct EntityId(pub i64);

impl From<i64> for EntityId {
    fn from(value: i64) -> Self {
        EntityId(value)
    }
}

impl TryFrom<EntityId> for usize {
    type Error = AttributeStoreError;

    fn try_from(value: EntityId) -> Result<Self, Self::Error> {
        let EntityId(database_id) = value;
        usize::try_from(database_id)
            .map_err(|_| AttributeStoreError::EntityNotFound(EntityLocator::EntityId(value)))
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct Symbol(String);

impl TryFrom<String> for Symbol {
    type Error = AttributeStoreError;

    fn try_from(string: String) -> Result<Self, Self::Error> {
        static SYMBOL_REGEX_CELL: OnceLock<Regex> = OnceLock::new();
        let symbol_regex = SYMBOL_REGEX_CELL.get_or_init(|| {
            Regex::new(r#"^[[:print:]--[\\"]]{1,60}$"#).expect("Failed to compile symbol regex")
        });

        if !symbol_regex.is_match(&string) {
            Err(AttributeStoreError::InvalidSymbolName(string))
        } else {
            Ok(Symbol(string))
        }
    }
}

impl TryFrom<&str> for Symbol {
    type Error = AttributeStoreError;

    #[inline]
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Symbol::try_from(value.to_string())
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        let Symbol(inner) = value;
        inner
    }
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum EntityLocator {
    EntityId(EntityId),
    Symbol(Symbol),
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Entity {
    pub entity_id: EntityId,
    // Should the key here be InternalEntityId?
    pub attributes: HashMap<Symbol, AttributeValue>,
}

impl Entity {
    pub fn to_entity_row<'a, I: IntoIterator<Item = &'a Symbol>>(
        &self,
        attribute_types: I,
    ) -> EntityRow {
        static ENTITY_ID_SYMBOL_CELL: OnceLock<Symbol> = OnceLock::new();
        let entity_id_symbol =
            ENTITY_ID_SYMBOL_CELL.get_or_init(|| BootstrapSymbol::EntityId.into());
        EntityRow {
            values: attribute_types
                .into_iter()
                .map(|attribute_type| {
                    if attribute_type == entity_id_symbol {
                        Some(AttributeValue::EntityId(self.entity_id))
                    } else {
                        self.attributes.get(attribute_type).cloned()
                    }
                })
                .collect(),
        }
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub enum AttributeValue {
    String(String),
    EntityId(EntityId),
    Bytes(Vec<u8>),
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct EntityQuery {
    pub root: EntityQueryNode,
    pub attribute_types: Vec<Symbol>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum EntityQueryNode {
    MatchAll(MatchAllQueryNode),
}

impl EntityQueryNode {
    pub fn to_predicate(&self) -> impl Fn(&&Entity) -> bool {
        match self {
            EntityQueryNode::MatchAll(_) => {
                fn predicate(_: &&Entity) -> bool {
                    true
                }
                predicate
            }
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct EntityRow {
    pub values: Vec<Option<AttributeValue>>,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub struct MatchAllQueryNode;

#[async_trait]
pub trait AttributeStore: Send + Sync + 'static {
    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError>;

    async fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError>;
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum BootstrapSymbol {
    EntityId,
    SymbolName,
    ValueType,
    ValueTypeEnum(ValueType),
}

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum ValueType {
    Text,
    EntityReference,
    Bytes,
}

impl From<BootstrapSymbol> for EntityId {
    #[inline]
    fn from(value: BootstrapSymbol) -> Self {
        match value {
            BootstrapSymbol::EntityId => EntityId(0),
            BootstrapSymbol::SymbolName => EntityId(1),
            BootstrapSymbol::ValueType => EntityId(2),
            BootstrapSymbol::ValueTypeEnum(value_type) => EntityId::from(value_type),
        }
    }
}

impl From<ValueType> for EntityId {
    #[inline]
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Text => EntityId(3),
            ValueType::EntityReference => EntityId(4),
            ValueType::Bytes => EntityId(5),
        }
    }
}

impl From<BootstrapSymbol> for Symbol {
    fn from(value: BootstrapSymbol) -> Self {
        match value {
            BootstrapSymbol::EntityId => Symbol("@id".to_string()),
            BootstrapSymbol::SymbolName => Symbol("@symbolName".to_string()),
            BootstrapSymbol::ValueType => Symbol("@valueType".to_string()),
            BootstrapSymbol::ValueTypeEnum(value_type) => Symbol::from(value_type),
        }
    }
}

impl From<ValueType> for Symbol {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Text => Symbol("@valueType/text".to_string()),
            ValueType::EntityReference => Symbol("@valueType/entityRef".to_string()),
            ValueType::Bytes => Symbol("@valueType/bytes".to_string()),
        }
    }
}

impl BootstrapSymbol {
    fn value_type(self: BootstrapSymbol) -> Option<EntityId> {
        match self {
            BootstrapSymbol::EntityId => Some(ValueType::EntityReference.into()),
            BootstrapSymbol::SymbolName => Some(ValueType::Text.into()),
            BootstrapSymbol::ValueType => Some(ValueType::EntityReference.into()),
            BootstrapSymbol::ValueTypeEnum(_) => None,
        }
    }
}

impl From<BootstrapSymbol> for Entity {
    fn from(value: BootstrapSymbol) -> Self {
        let symbol: Symbol = value.into();
        let mut attributes = HashMap::from([(
            BootstrapSymbol::SymbolName.into(),
            AttributeValue::String(symbol.into()),
        )]);
        if let Some(value_type_entity_id) = value.value_type() {
            attributes.insert(
                BootstrapSymbol::ValueType.into(),
                AttributeValue::EntityId(value_type_entity_id),
            );
        }

        Entity {
            entity_id: value.into(),
            attributes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_symbols() {
        use AttributeStoreError::InvalidSymbolName;

        assert_matches!(Symbol::try_from(r"ab\c"), Err(InvalidSymbolName(_)));
        assert_matches!(Symbol::try_from(r#"ab"c"#), Err(InvalidSymbolName(_)));
        assert_matches!(Symbol::try_from(""), Err(InvalidSymbolName(_)));
        assert_matches!(
            Symbol::try_from("0123456789".repeat(7)),
            Err(InvalidSymbolName(_))
        );
    }

    #[test]
    fn valid_symbols() {
        assert_eq!(Symbol::try_from("abc").unwrap(), Symbol("abc".to_string()));
        assert_eq!(Symbol::try_from("@id").unwrap(), Symbol("@id".to_string()));
        assert_eq!(
            Symbol::try_from("@valueType/text").unwrap(),
            Symbol("@valueType/text".to_string())
        );
    }
}
