use crate::store::{
    AttributeStore, AttributeStoreError, AttributeValue, BootstrapSymbol, Entity, EntityId,
    EntityLocator, EntityQuery, EntityRow, Symbol, ValueType,
};
use async_trait::async_trait;
use std::sync::Mutex;
use tracing::Level;

#[derive(Debug)]
pub struct InMemoryAttributeStore {
    entities: Mutex<Vec<Entity>>,
}

impl InMemoryAttributeStore {
    pub fn new() -> Self {
        let entities: Vec<Entity> = Self::bootstrap_entities();

        for (idx, entity) in entities.iter().enumerate() {
            let EntityId(database_id) = entity.entity_id;
            assert_eq!(usize::try_from(database_id).unwrap(), idx);
        }
        InMemoryAttributeStore {
            entities: Mutex::new(entities),
        }
    }

    fn bootstrap_entities() -> Vec<Entity> {
        vec![
            BootstrapSymbol::EntityId.into(),
            BootstrapSymbol::SymbolName.into(),
            BootstrapSymbol::ValueType.into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::Text).into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::EntityReference).into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::Bytes).into(),
        ]
    }
}

#[async_trait]
impl AttributeStore for InMemoryAttributeStore {
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreError::*;

        log::trace!("Received get_entity request");

        let locked_entities = self
            .entities
            .lock()
            .map_err(|_| InternalError("task failed while holding lock"))?;
        let entity = match entity_locator {
            EntityLocator::EntityId(entity_id) => locked_entities.get(usize::try_from(*entity_id)?),
            EntityLocator::Symbol(symbol) => {
                let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
                let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                locked_entities.iter().find(|entity| {
                    entity
                        .attributes
                        .get(&symbol_name_symbol)
                        .is_some_and(|attribute_value| {
                            attribute_value.eq(&expected_attribute_value)
                        })
                })
            }
        }
        .ok_or_else(|| EntityNotFound(entity_locator.clone()))?;

        Ok(entity.clone())
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError> {
        use AttributeStoreError::*;

        log::trace!("Received query_entities request");

        let locked_entities = self
            .entities
            .lock()
            .map_err(|_| InternalError("task failed while holding lock"))?;

        let entity_rows = locked_entities
            .iter()
            .filter(entity_query.root.to_predicate())
            .map(|entity| entity.to_entity_row(&entity_query.attribute_types))
            .collect();

        Ok(entity_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EntityQueryNode, MatchAllQueryNode};

    #[tokio::test]
    async fn can_fetch_by_entity_id() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::EntityId(BootstrapSymbol::EntityId.into()))
            .await
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[tokio::test]
    async fn can_fetch_by_symbol() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::Symbol(BootstrapSymbol::EntityId.into()))
            .await
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[tokio::test]
    async fn can_query_all() {
        let store = InMemoryAttributeStore::new();
        let entities = store
            .query_entities(&EntityQuery {
                attribute_types: vec![
                    BootstrapSymbol::EntityId.into(),
                    BootstrapSymbol::SymbolName.into(),
                ],
                root: EntityQueryNode::MatchAll(MatchAllQueryNode),
            })
            .await
            .unwrap();
        assert_eq!(
            entities,
            InMemoryAttributeStore::bootstrap_entities()
                .into_iter()
                .map(|entity| EntityRow {
                    values: vec![
                        Some(AttributeValue::EntityId(entity.entity_id)),
                        entity
                            .attributes
                            .get(&BootstrapSymbol::SymbolName.into())
                            .cloned()
                    ]
                })
                .collect::<Vec<_>>()
        );
    }
}
