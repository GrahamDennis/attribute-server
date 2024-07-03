use crate::store::{
    AttributeStore, AttributeStoreError, AttributeValue, BootstrapSymbol, Entity, EntityId,
    EntityLocator, Symbol, ValueType,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct InMemoryAttributeStore {
    entities: Mutex<HashMap<EntityId, Entity>>,
}

impl InMemoryAttributeStore {
    pub fn new() -> Self {
        let mut entities: HashMap<EntityId, Entity> = HashMap::new();
        for entity in Self::bootstrap_entities().into_iter() {
            entities.insert(entity.entity_id, entity);
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
    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreError::*;

        let locked_entities = self
            .entities
            .lock()
            .map_err(|_| InternalError("task failed while holding lock"))?;
        let entity =
            match entity_locator {
                EntityLocator::EntityId(entity_id) => locked_entities.get(entity_id),
                EntityLocator::Symbol(symbol) => {
                    let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
                    let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                    locked_entities
                        .iter()
                        .map(|(_, entity)| entity)
                        .find(|entity| {
                            entity.attributes.get(&symbol_name_symbol).is_some_and(
                                |attribute_value| attribute_value.eq(&expected_attribute_value),
                            )
                        })
                }
            }
            .ok_or_else(|| EntityNotFound(entity_locator.clone()))?;

        Ok(entity.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
