mod types;

use derive_more::{Display, From};

use binding_macro::{cycles, read, service, write};
use protocol::fixed_codec::FixedCodec;
use protocol::traits::{
    Service as ServiceTrait, ServiceSDK, StoreArray, StoreBool, StoreMap, StoreString, StoreUint64,
};
use protocol::types::{Bytes, Hash, Metadata, ServiceContext, METADATA_KEY};
use protocol::{ProtocolError, ProtocolErrorKind, ProtocolResult};

use crate::types::{GetContentPayload, GetContentResponse, StorePayload, StoreResponse};

pub struct Service<SDK: ServiceSDK> {
    sdk: SDK,
    messages: Box<dyn StoreMap<Hash, String>>,
}

#[service]
impl<SDK: 'static + ServiceSDK> Service<SDK> {
    pub fn new(mut sdk: SDK) -> ProtocolResult<Self> {
        let messages: Box<dyn StoreMap<Hash, String>> = sdk.alloc_or_recover_map("messages")?;
        Ok(Self { sdk, messages })
    }

    #[cycles(210_00)]
    #[write]
    fn store(
        &mut self,
        ctx: ServiceContext,
        payload: StorePayload,
    ) -> ProtocolResult<StoreResponse> {
        let message = payload.message.clone();
        let id = Hash::digest(Bytes::from(message + &ctx.get_caller().as_hex()));
        self.messages.insert(id.clone(), payload.message)?;

        Ok(StoreResponse { id })
    }

    #[cycles(210_00)]
    #[read]
    fn get(
        &self,
        ctx: ServiceContext,
        payload: GetContentPayload,
    ) -> ProtocolResult<GetContentResponse> {
        let message = self.messages.get(&payload.id)?;
        Ok(GetContentResponse { message })
    }
}
