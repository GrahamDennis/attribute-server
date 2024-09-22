use crate::connection::{MavlinkClient, MavlinkNetwork};
use async_trait::async_trait;
use mavio::dialects::common::messages::{
    MissionCount, MissionItemInt, MissionRequestInt, MissionRequestList,
};
use mavio::protocol::{MaybeVersioned, Versioned};

#[async_trait]
pub trait MissionProtocol {
    async fn request_list(
        &mut self,
        request_list: MissionRequestList,
    ) -> anyhow::Result<MissionCount>;
    async fn request_int(
        &mut self,
        request_int: MissionRequestInt,
    ) -> anyhow::Result<MissionItemInt>;
}

#[async_trait]
impl<V: Versioned> MissionProtocol for MavlinkClient<V> {
    async fn request_list(
        &mut self,
        request_list: MissionRequestList,
    ) -> anyhow::Result<MissionCount> {
        // FIXME: the response can be a MISSION_ACK with an error code
        let system_id = self.system_id;
        let component_id = self.component_id;
        let response_predicate = |response: &MissionCount| {
            response.target_system == system_id && response.target_component == component_id
        };
        self.send_and_await_response_with_predicate(request_list, response_predicate)
            .await
    }

    async fn request_int(
        &mut self,
        request_int: MissionRequestInt,
    ) -> anyhow::Result<MissionItemInt> {
        let system_id = self.system_id;
        let component_id = self.component_id;
        let response_predicate = |response: &MissionItemInt| {
            response.target_system == system_id && response.target_component == component_id
        };
        // FIXME: the response can be a MISSION_ACK with an error code
        self.send_and_await_response_with_predicate(request_int, response_predicate)
            .await
    }
}
