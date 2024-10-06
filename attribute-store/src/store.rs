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
use std::sync::{Arc, LazyLock};
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

#[derive(Eq, PartialEq, Hash, Debug, Copy, Clone, Ord, PartialOrd)]
pub struct EntityVersion(pub i64);

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

static SYMBOL_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"^[[:print:]--[\\"]]{1,60}$"#).expect("Failed to compile symbol regex")
});

impl TryFrom<Cow<'static, str>> for Symbol {
    type Error = AttributeStoreError;

    fn try_from(string: Cow<'static, str>) -> Result<Self, Self::Error> {
        use AttributeStoreErrorKind::*;

        if !SYMBOL_REGEX.is_match(&string) {
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
    pub entity_version: EntityVersion,
    // Should the key here be InternalEntityId?
    pub attributes: HashMap<Symbol, AttributeValue>,
}

static ENTITY_ID_SYMBOL: LazyLock<Symbol> = LazyLock::new(|| BootstrapSymbol::EntityId.into());

impl Entity {
    pub fn to_entity_row<'a, I: IntoIterator<Item = &'a Symbol>>(
        &self,
        attribute_types: I,
    ) -> EntityRow {
        EntityRow {
            values: attribute_types
                .into_iter()
                .map(|attribute_type| {
                    if attribute_type == ENTITY_ID_SYMBOL.deref() {
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
pub struct EntityRowQuery {
    #[garde(skip)]
    pub root: EntityQueryNode,
    #[garde(inner(custom(is_known_attribute_type)))]
    pub attribute_types: Vec<Symbol>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct EntityRowQueryResult {
    pub entity_rows: Vec<EntityRow>,
    pub entity_version: EntityVersion,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct EntityQuery {
    pub root: EntityQueryNode,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct EntityQueryResult {
    pub entities: Vec<Entity>,
    pub entity_version: EntityVersion,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum EntityQueryNode {
    MatchAll(MatchAllQueryNode),
    MatchNone(MatchNoneQueryNode),
    And(AndQueryNode),
    Or(OrQueryNode),
    HasAttributeTypes(HasAttributeTypesNode),
}

impl EntityQueryNode {
    pub fn matches(&self, entity: &Entity) -> bool {
        match self {
            EntityQueryNode::MatchAll(_) => true,
            EntityQueryNode::MatchNone(_) => false,
            EntityQueryNode::And(AndQueryNode { clauses }) => {
                clauses.iter().all(|item| item.matches(entity))
            }
            EntityQueryNode::Or(OrQueryNode { clauses }) => {
                clauses.iter().any(|item| item.matches(entity))
            }
            EntityQueryNode::HasAttributeTypes(HasAttributeTypesNode { attribute_types }) => {
                attribute_types
                    .iter()
                    .all(|attribute_type| entity.attributes.contains_key(attribute_type))
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

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct HasAttributeTypesNode {
    pub attribute_types: Vec<Symbol>,
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
    #[garde(skip)]
    pub attribute_type: AttributeType,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct WatchEntitiesRequest {
    pub query: EntityQueryNode,
    pub send_initial_events: bool,
}

#[derive(Eq, PartialEq, Debug, Clone, garde::Validate)]
#[garde(context(AttributeTypes))]
pub struct WatchEntityRowsRequest {
    #[garde(skip)]
    pub query: EntityQueryNode,
    #[garde(inner(custom(is_known_attribute_type)))]
    pub attribute_types: Vec<Symbol>,
    #[garde(skip)]
    pub send_initial_events: bool,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct WatchEntitiesEvent {
    pub entity_version: EntityVersion,
    pub before: Option<Arc<Entity>>,
    pub after: Option<Arc<Entity>>,
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub struct WatchEntityRowsEvent {
    pub entity_version: EntityVersion,
    pub before: Option<EntityRow>,
    pub after: Option<EntityRow>,
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
    ) -> Result<EntityQueryResult, AttributeStoreError>;

    async fn query_entity_rows(
        &self,
        entity_row_query: &EntityRowQuery,
    ) -> Result<EntityRowQueryResult, AttributeStoreError>;

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
    ) -> Result<EntityQueryResult, AttributeStoreError>;

    fn query_entity_rows(
        &self,
        entity_row_query: &EntityRowQuery,
    ) -> Result<EntityRowQueryResult, AttributeStoreError>;

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
    ) -> Result<EntityQueryResult, AttributeStoreError> {
        self.lock().query_entities(entity_query)
    }

    async fn query_entity_rows(
        &self,
        entity_query: &EntityRowQuery,
    ) -> Result<EntityRowQueryResult, AttributeStoreError> {
        self.lock().query_entity_rows(entity_query)
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
            entity_version: EntityVersion(0),
            attributes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_symbols() {
        use AttributeStoreErrorKind::InvalidSymbolName;

        assert_matches!(
            Symbol::try_from(r"ab\c").unwrap_err().kind,
            InvalidSymbolName(_)
        );
        assert_matches!(
            Symbol::try_from(r#"ab"c"#).unwrap_err().kind,
            InvalidSymbolName(_)
        );
        assert_matches!(Symbol::try_from("").unwrap_err().kind, InvalidSymbolName(_));
        assert_matches!(
            Symbol::try_from("0123456789".repeat(7)).unwrap_err().kind,
            InvalidSymbolName(_)
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
