use async_trait::async_trait;
use parking_lot::Mutex;
use regex::Regex;
use std::borrow::Cow;
use std::boxed::Box;
use std::collections::HashMap;
use std::convert::Into;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::sync::OnceLock;
use thiserror::Error;
use tokio::sync::broadcast::Receiver;

#[derive(Error, Debug)]
pub enum AttributeStoreErrorKind {
    #[error("name `{0}` is not a valid symbol name")]
    InvalidSymbolName(Cow<'static, str>),
    #[error("entity not found (locator: `{0:?}`)")]
    EntityNotFound(EntityLocator),
    #[error("attribute type `{0:?}` already exists")]
    AttributeTypeAlreadyExists(Entity),
    #[error("invalid value type entity ID: `{0:?}`")]
    InvalidValueType(EntityId),
    #[error("validation error")]
    ValidationError(#[from] garde::Report),
    #[error(
        "update not idempotent; missing attribute to update: `{missing_attribute_to_update:?}` \
    given locator `{entity_locator:?}`"
    )]
    UpdateNotIdempotent {
        missing_attribute_to_update: AttributeToUpdate,
        entity_locator: EntityLocator,
    },
    #[error("internal error: `{message}`")]
    Other {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[derive(Debug)]
pub struct AttributeStoreError {
    pub kind: AttributeStoreErrorKind,
    // put SpanTrace and similar here
}

impl Display for AttributeStoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // Is this correct?
        std::fmt::Display::fmt(&self.kind, f)
    }
}

impl std::error::Error for AttributeStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

impl<T: Into<AttributeStoreErrorKind>> From<T> for AttributeStoreError {
    fn from(value: T) -> Self {
        AttributeStoreError { kind: value.into() }
    }
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
        use AttributeStoreErrorKind::*;
        let EntityId(database_id) = value;
        let entity_id = usize::try_from(database_id)
            .map_err(|_| EntityNotFound(EntityLocator::EntityId(value)))?;
        Ok(entity_id)
    }
}

#[derive(Eq, PartialEq, Hash, Debug, Clone)]
pub struct Symbol(Cow<'static, str>);

impl TryFrom<Cow<'static, str>> for Symbol {
    type Error = AttributeStoreError;

    fn try_from(string: Cow<'static, str>) -> Result<Self, Self::Error> {
        use AttributeStoreErrorKind::*;
        static SYMBOL_REGEX_CELL: OnceLock<Regex> = OnceLock::new();
        let symbol_regex = SYMBOL_REGEX_CELL.get_or_init(|| {
            Regex::new(r#"^[[:print:]--[\\"]]{1,60}$"#).expect("Failed to compile symbol regex")
        });

        if !symbol_regex.is_match(&string) {
            Err(InvalidSymbolName(string))?
        } else {
            Ok(Symbol(string))
        }
    }
}

impl TryFrom<&'static str> for Symbol {
    type Error = AttributeStoreError;

    #[inline]
    fn try_from(value: &'static str) -> Result<Self, Self::Error> {
        Symbol::try_from(Cow::from(value))
    }
}

impl TryFrom<String> for Symbol {
    type Error = AttributeStoreError;

    #[inline]
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Symbol::try_from(Cow::from(value))
    }
}

impl From<Symbol> for String {
    fn from(value: Symbol) -> Self {
        let Symbol(inner) = value;
        inner.into_owned()
    }
}

impl Deref for Symbol {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        let Symbol(inner) = self;
        inner
    }
}

pub type AttributeTypes = HashMap<Symbol, ValueType>;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct AttributeType {
    pub symbol: Symbol,
    pub value_type: ValueType,
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

#[derive(Eq, PartialEq, Debug, Clone, garde::Validate)]
#[garde(context(AttributeTypes))]
pub struct EntityQuery {
    #[garde(skip)]
    pub root: EntityQueryNode,
    #[garde(inner(custom(is_known_attribute_type)))]
    pub attribute_types: Vec<Symbol>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum EntityQueryNode {
    MatchAll(MatchAllQueryNode),
    MatchNone(MatchNoneQueryNode),
    And(AndQueryNode),
    Or(OrQueryNode),
}

fn match_all(_: &&Entity) -> bool {
    true
}
fn match_none(_: &&Entity) -> bool {
    false
}

impl EntityQueryNode {
    pub fn to_predicate(&self) -> Box<dyn Fn(&&Entity) -> bool> {
        match self {
            EntityQueryNode::MatchAll(_) => Box::new(match_all),
            EntityQueryNode::MatchNone(_) => Box::new(match_none),
            EntityQueryNode::And(AndQueryNode { clauses }) => {
                let predicates: Vec<_> = clauses
                    .into_iter()
                    .map(|clause| clause.to_predicate())
                    .collect();
                let predicate =
                    move |entity: &&Entity| -> bool { predicates.iter().all(|p| p(entity)) };
                Box::new(predicate)
            }
            EntityQueryNode::Or(OrQueryNode { clauses }) => {
                let predicates: Vec<_> = clauses
                    .into_iter()
                    .map(|clause| clause.to_predicate())
                    .collect();
                let predicate =
                    move |entity: &&Entity| -> bool { predicates.iter().any(|p| p(entity)) };
                Box::new(predicate)
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

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub struct MatchNoneQueryNode;

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct AndQueryNode {
    pub clauses: Vec<EntityQueryNode>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct OrQueryNode {
    pub clauses: Vec<EntityQueryNode>,
}

#[derive(Eq, PartialEq, Debug, Clone, garde::Validate)]
#[garde(context(AttributeTypes))]
pub struct AttributeToUpdate {
    #[garde(custom(is_known_attribute_type), custom(not_immutable_attribute_type))]
    pub symbol: Symbol,
    #[garde(custom(attribute_value_matches_attribute_type(&self.symbol)))]
    pub value: Option<AttributeValue>,
}

fn attribute_value_matches_attribute_type(
    symbol: &Symbol,
) -> impl FnOnce(&Option<AttributeValue>, &AttributeTypes) -> garde::Result + '_ {
    move |value, attribute_types| {
        let expected_attribute_type = attribute_types
            .get(symbol)
            .ok_or_else(|| garde::Error::new("cannot find value type for attribute type"))?;
        match (value, expected_attribute_type) {
            (None, _) => (),
            (Some(AttributeValue::String(_)), ValueType::Text) => (),
            (Some(AttributeValue::EntityId(_)), ValueType::EntityReference) => (),
            (Some(AttributeValue::Bytes(_)), ValueType::Bytes) => (),
            _ => {
                return Err(garde::Error::new(format!(
                    "incorrect value type, expected {:?}",
                    expected_attribute_type
                )));
            }
        };

        Ok(())
    }
}

fn not_immutable_attribute_type(symbol: &Symbol, _: &AttributeTypes) -> garde::Result {
    let value_type_symbol: Symbol = BootstrapSymbol::ValueType.into();
    if *symbol == value_type_symbol {
        return Err(garde::Error::new("immutable attribute type"));
    }

    Ok(())
}

fn is_known_attribute_type(symbol: &Symbol, attribute_types: &AttributeTypes) -> garde::Result {
    if !attribute_types.contains_key(symbol) {
        return Err(garde::Error::new("unregistered attribute type"));
    }

    Ok(())
}

fn is_new_attribute_type(
    attribute_type: &AttributeType,
    attribute_types: &AttributeTypes,
) -> garde::Result {
    if attribute_types.contains_key(&attribute_type.symbol) {
        return Err(garde::Error::new("attribute type already exists"));
    }

    Ok(())
}

#[derive(Eq, PartialEq, Debug, Clone, garde::Validate)]
#[garde(context(AttributeTypes))]
pub struct UpdateEntityRequest {
    #[garde(skip)]
    pub entity_locator: EntityLocator,
    #[garde(dive)]
    pub attributes_to_update: Vec<AttributeToUpdate>,
}

#[derive(Eq, PartialEq, Debug, Clone, garde::Validate)]
#[garde(context(AttributeTypes))]
pub struct CreateAttributeTypeRequest {
    #[garde(custom(is_new_attribute_type))]
    pub attribute_type: AttributeType,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct WatchEntitiesRequest {}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct WatchEntitiesEvent {
    pub before: Option<Entity>,
    pub after: Option<Entity>,
}

#[async_trait]
pub trait ThreadSafeAttributeStore: Send + Sync + 'static {
    async fn create_attribute_type(
        &self,
        create_attribute_type_request: &CreateAttributeTypeRequest,
    ) -> Result<Entity, AttributeStoreError>;

    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError>;

    async fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError>;

    async fn update_entity(
        &self,
        update_entity_request: &UpdateEntityRequest,
    ) -> Result<Entity, AttributeStoreError>;

    fn watch_entities_receiver(&self) -> Receiver<WatchEntitiesEvent>;
}

pub trait AttributeStore {
    fn create_attribute_type(
        &mut self,
        create_attribute_type_request: &CreateAttributeTypeRequest,
    ) -> Result<Entity, AttributeStoreError>;

    fn get_entity(&self, entity_locator: &EntityLocator) -> Result<Entity, AttributeStoreError>;

    fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError>;

    fn update_entity(
        &mut self,
        update_entity_request: &UpdateEntityRequest,
    ) -> Result<Entity, AttributeStoreError>;

    fn watch_entities_receiver(&self) -> Receiver<WatchEntitiesEvent>;
}

#[async_trait]
impl<T: AttributeStore + Send + 'static> ThreadSafeAttributeStore for Mutex<T> {
    async fn create_attribute_type(
        &self,
        create_attribute_type_request: &CreateAttributeTypeRequest,
    ) -> Result<Entity, AttributeStoreError> {
        self.lock()
            .create_attribute_type(create_attribute_type_request)
    }

    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError> {
        self.lock().get_entity(entity_locator)
    }

    async fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError> {
        self.lock().query_entities(entity_query)
    }

    async fn update_entity(
        &self,
        update_entity_request: &UpdateEntityRequest,
    ) -> Result<Entity, AttributeStoreError> {
        self.lock().update_entity(update_entity_request)
    }

    fn watch_entities_receiver(&self) -> Receiver<WatchEntitiesEvent> {
        self.lock().watch_entities_receiver()
    }
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

impl TryFrom<EntityId> for ValueType {
    type Error = AttributeStoreError;

    fn try_from(value: EntityId) -> Result<Self, Self::Error> {
        use AttributeStoreErrorKind::*;
        use ValueType::*;
        match value {
            EntityId(3) => Ok(Text),
            EntityId(4) => Ok(EntityReference),
            EntityId(5) => Ok(Bytes),
            other_entity_id => Err(InvalidValueType(other_entity_id))?,
        }
    }
}

impl From<BootstrapSymbol> for Symbol {
    fn from(value: BootstrapSymbol) -> Self {
        match value {
            BootstrapSymbol::EntityId => Symbol("@id".into()),
            BootstrapSymbol::SymbolName => Symbol("@symbolName".into()),
            BootstrapSymbol::ValueType => Symbol("@valueType".into()),
            BootstrapSymbol::ValueTypeEnum(value_type) => Symbol::from(value_type),
        }
    }
}

impl From<ValueType> for Symbol {
    fn from(value: ValueType) -> Self {
        match value {
            ValueType::Text => Symbol("@valueType/text".into()),
            ValueType::EntityReference => Symbol("@valueType/entityRef".into()),
            ValueType::Bytes => Symbol("@valueType/bytes".into()),
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
        assert_eq!(Symbol::try_from("abc").unwrap(), Symbol("abc".into()));
        assert_eq!(Symbol::try_from("@id").unwrap(), Symbol("@id".into()));
        assert_eq!(
            Symbol::try_from("@valueType/text").unwrap(),
            Symbol("@valueType/text".into())
        );
    }
}
