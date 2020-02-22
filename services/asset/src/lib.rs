#[cfg(test)]
mod tests;
pub mod types;

use bytes::Bytes;
use derive_more::{Display, From};

use binding_macro::{cycles, genesis, service, write};
use protocol::traits::{ExecutorParams, ServiceSDK, StoreMap};
use protocol::types::{Hash, ServiceContext};
use protocol::{ProtocolError, ProtocolErrorKind, ProtocolResult};

use crate::types::{
    Asset, Balance, CreateAssetPayload, GetAssetPayload, GetBalancePayload, GetBalanceResponse,
    InitGenesisPayload, ModifyBalancePayload, TransferEvent, TransferPayload,
};

const ADMISSION_TOKEN: Bytes = Bytes::from_static(b"dex_token");
const ASSETS_KEY: &str = "assets";

pub struct AssetService<SDK> {
    sdk: SDK,
    assets: Box<dyn StoreMap<Hash, Asset>>,
}

#[service]
impl<SDK: ServiceSDK> AssetService<SDK> {
    pub fn new(mut sdk: SDK) -> ProtocolResult<Self> {
        let assets: Box<dyn StoreMap<Hash, Asset>> = sdk.alloc_or_recover_map(ASSETS_KEY)?;

        Ok(Self { sdk, assets })
    }

    #[genesis]
    fn init_genesis(&mut self, payload: InitGenesisPayload) -> ProtocolResult<()> {
        let asset = Asset {
            id: payload.id,
            name: payload.name,
            symbol: payload.symbol,
            supply: payload.supply,
            issuer: payload.issuer.clone(),
        };

        self.assets.insert(asset.id.clone(), asset.clone())?;

        let balance = Balance {
            current: payload.supply,
            locked: 0,
        };

        self.sdk.set_account_value(&asset.issuer, asset.id, balance)
    }

    #[cycles(210_00)]
    #[write]
    fn create_asset(
        &mut self,
        ctx: ServiceContext,
        payload: CreateAssetPayload,
    ) -> ProtocolResult<Asset> {
        let caller = ctx.get_caller();
        let payload_str = serde_json::to_string(&payload).map_err(AssetError::JsonParse)?;

        let id = Hash::digest(Bytes::from(payload_str + &caller.as_hex()));
        if self.assets.contains(&id)? {
            return Err(AssetError::AssetExisted { id }.into());
        }

        let asset = Asset {
            id: id.clone(),
            name: payload.name,
            symbol: payload.symbol,
            supply: payload.supply,
            issuer: caller.clone(),
        };
        self.assets.insert(id.clone(), asset.clone())?;

        let balance = Balance {
            current: payload.supply,
            locked: 0,
        };
        self.sdk.set_account_value(&caller, id, balance)?;

        let event_string = serde_json::to_string(&asset).map_err(AssetError::JsonParse)?;
        ctx.emit_event(event_string)?;

        Ok(asset)
    }

    #[cycles(100_00)]
    #[read]
    fn get_asset(&self, ctx: ServiceContext, payload: GetAssetPayload) -> ProtocolResult<Asset> {
        let asset = self.assets.get(&payload.id)?;
        Ok(asset)
    }

    #[cycles(100_00)]
    #[read]
    fn get_balance(
        &self,
        ctx: ServiceContext,
        payload: GetBalancePayload,
    ) -> ProtocolResult<GetBalanceResponse> {
        let balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)?
            .unwrap_or(Balance::default());
        Ok(GetBalanceResponse {
            asset_id: payload.asset_id,
            balance,
        })
    }

    #[cycles(210_00)]
    #[write]
    fn transfer(&mut self, ctx: ServiceContext, payload: TransferPayload) -> ProtocolResult<()> {
        let sub_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: ctx.get_caller(),
            value: payload.value,
        };
        self._sub_value(&sub_payload)?;

        let add_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.to.clone(),
            value: payload.value,
        };
        self._add_value(&add_payload)?;

        let event = TransferEvent {
            asset_id: payload.asset_id,
            from: ctx.get_caller(),
            to: payload.to,
            value: payload.value,
        };
        let event_json = serde_json::to_string(&event).map_err(AssetError::JsonParse)?;
        ctx.emit_event(event_json)
    }

    #[cycles(210_00)]
    #[write]
    fn add_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ProtocolResult<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return Err(AssetError::PermissionDenial.into());
        }

        self._add_value(&payload)
    }

    #[cycles(210_00)]
    #[write]
    fn sub_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ProtocolResult<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return Err(AssetError::PermissionDenial.into());
        }

        self._sub_value(&payload)
    }

    #[cycles(210_00)]
    #[write]
    fn lock(&mut self, ctx: ServiceContext, payload: ModifyBalancePayload) -> ProtocolResult<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return Err(AssetError::PermissionDenial.into());
        }

        if !self.assets.contains(&payload.asset_id)? {
            return Err(AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into());
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)?
            .unwrap_or(Balance::default());

        if balance.current < payload.value {
            return Err(AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.current,
            }
            .into());
        }

        balance.current = balance.current - payload.value;
        let (result, overflow) = balance.locked.overflowing_add(payload.value);
        if overflow {
            return Err(AssetError::U64Overflow.into());
        }

        balance.locked = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id, balance)
    }

    #[cycles(210_00)]
    #[write]
    fn unlock(&mut self, ctx: ServiceContext, payload: ModifyBalancePayload) -> ProtocolResult<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return Err(AssetError::PermissionDenial.into());
        }

        if !self.assets.contains(&payload.asset_id)? {
            return Err(AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into());
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)?
            .unwrap_or(Balance::default());

        if balance.locked < payload.value {
            return Err(AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.locked,
            }
            .into());
        }
        balance.locked = balance.locked - payload.value;
        let (result, overflow) = balance.current.overflowing_add(payload.value);
        if overflow {
            return Err(AssetError::U64Overflow.into());
        }

        balance.current = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id, balance)
    }

    fn _add_value(&mut self, payload: &ModifyBalancePayload) -> ProtocolResult<()> {
        if !self.assets.contains(&payload.asset_id)? {
            return Err(AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into());
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)?
            .unwrap_or(Balance::default());

        let (result, overflow) = balance.current.overflowing_add(payload.value);
        if overflow {
            return Err(AssetError::U64Overflow.into());
        }

        balance.current = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id.clone(), balance)
    }

    fn _sub_value(&mut self, payload: &ModifyBalancePayload) -> ProtocolResult<()> {
        if !self.assets.contains(&payload.asset_id)? {
            return Err(AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into());
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)?
            .unwrap_or(Balance::default());

        if balance.current < payload.value {
            return Err(AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.current,
            }
            .into());
        }

        balance.current = balance.current - payload.value;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id.clone(), balance)
    }
}

#[derive(Debug, Display, From)]
pub enum AssetError {
    #[display(fmt = "Parsing payload to json failed {:?}", _0)]
    JsonParse(serde_json::Error),

    #[display(fmt = "Asset {:?} already exists", id)]
    AssetExisted {
        id: Hash,
    },

    #[display(fmt = "Not found asset, id {:?}", id)]
    AssetNotExist {
        id: Hash,
    },

    #[display(fmt = "Not found asset, expect {:?} real {:?}", wanted, had)]
    InsufficientBalance {
        wanted: u64,
        had: u64,
    },

    U64Overflow,

    PermissionDenial,
}

impl std::error::Error for AssetError {}

impl From<AssetError> for ProtocolError {
    fn from(err: AssetError) -> ProtocolError {
        ProtocolError::new(ProtocolErrorKind::Service, Box::new(err))
    }
}
