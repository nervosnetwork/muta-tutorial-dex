#[cfg(test)]
mod tests;
pub mod types;

use std::convert::From;

use bytes::Bytes;
use derive_more::Display;

use binding_macro::{cycles, genesis, service, write};
use protocol::traits::{ExecutorParams, ServiceResponse, ServiceSDK, StoreMap};
use protocol::types::{Hash, ServiceContext};

use crate::types::{
    Asset, Balance, CreateAssetPayload, GetAssetPayload, GetBalancePayload, GetBalanceResponse,
    InitGenesisPayload, ModifyBalancePayload, TransferEvent, TransferPayload,
};

/*
call a method which returns ServiceResponse.
if the return is ok, get the data,
if the return is error, 'return' it
 */
macro_rules! call_and_parse_service_response {
    ($self: expr, $method: ident) => {{
        let res: ServiceResponse<_> = $self.$method();
        if res.is_error() {
            return ServiceResponse::from_error(res.code, res.error_message);
        } else {
            res.succeed_data
        }
    }};
    ($self: expr, $method: ident, $payload: expr) => {{
        let res: ServiceResponse<_> = $self.$method($payload);
        if res.is_error() {
            return ServiceResponse::from_error(res.code, res.error_message);
        } else {
            res.succeed_data
        }
    }};
}

macro_rules! serde_json_string {
    ($payload: expr) => {
        match serde_json::to_string(&$payload).map_err(AssetError::JsonParse) {
            Ok(s) => s,
            Err(e) => return e.into(),
        };
    };
}

const ADMISSION_TOKEN: Bytes = Bytes::from_static(b"dex_token");
const ASSETS_KEY: &str = "assets";

pub struct AssetService<SDK> {
    sdk: SDK,
    assets: Box<dyn StoreMap<Hash, Asset>>,
}

pub trait AssetFacade {
    fn lock(&mut self, ctx: ServiceContext, payload: ModifyBalancePayload) -> ServiceResponse<()>;

    fn unlock(&mut self, ctx: ServiceContext, payload: ModifyBalancePayload)
        -> ServiceResponse<()>;

    fn add_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ServiceResponse<()>;

    fn sub_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ServiceResponse<()>;
}

// this is for other service
impl<SDK: ServiceSDK> AssetFacade for AssetService<SDK> {
    fn add_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ServiceResponse<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return AssetError::PermissionDenial.into();
        }

        self._add_value(&payload)
    }

    fn sub_value(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ServiceResponse<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return AssetError::PermissionDenial.into();
        }

        self._sub_value(&payload)
    }

    fn lock(&mut self, ctx: ServiceContext, payload: ModifyBalancePayload) -> ServiceResponse<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return AssetError::PermissionDenial.into();
        }

        if !self.assets.contains(&payload.asset_id) {
            return AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into();
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)
            .unwrap_or(Balance::default());

        if balance.current < payload.value {
            return AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.current,
            }
            .into();
        }

        balance.current = balance.current - payload.value;
        let (result, overflow) = balance.locked.overflowing_add(payload.value);
        if overflow {
            return AssetError::U64Overflow.into();
        }

        balance.locked = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id, balance);
        ServiceResponse::from_succeed(())
    }

    fn unlock(
        &mut self,
        ctx: ServiceContext,
        payload: ModifyBalancePayload,
    ) -> ServiceResponse<()> {
        let extra = ctx.get_extra().expect("Caller should have admission token");
        if extra != ADMISSION_TOKEN {
            return AssetError::PermissionDenial.into();
        }

        if !self.assets.contains(&payload.asset_id) {
            return AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into();
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)
            .unwrap_or(Balance::default());

        if balance.locked < payload.value {
            return AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.locked,
            }
            .into();
        }
        balance.locked = balance.locked - payload.value;
        let (result, overflow) = balance.current.overflowing_add(payload.value);
        if overflow {
            return AssetError::U64Overflow.into();
        }

        balance.current = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id, balance);
        ServiceResponse::from_succeed(())
    }
}

//this is for outside

#[service]
impl<SDK: ServiceSDK> AssetService<SDK> {
    pub fn new(mut sdk: SDK) -> Self {
        let assets: Box<dyn StoreMap<Hash, Asset>> = sdk.alloc_or_recover_map(ASSETS_KEY);

        Self { sdk, assets }
    }

    #[genesis]
    fn init_genesis(&mut self, payload: InitGenesisPayload) {
        let asset = Asset {
            id: payload.id,
            name: payload.name,
            symbol: payload.symbol,
            supply: payload.supply,
            issuer: payload.issuer.clone(),
        };

        self.assets.insert(asset.id.clone(), asset.clone());

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
    ) -> ServiceResponse<Asset> {
        let caller = ctx.get_caller();
        let payload_str = serde_json_string!(payload);

        let id = Hash::digest(Bytes::from(payload_str + &caller.to_string()));
        if self.assets.contains(&id) {
            return AssetError::AssetExisted { id }.into();
        }

        let asset = Asset {
            id: id.clone(),
            name: payload.name,
            symbol: payload.symbol,
            supply: payload.supply,
            issuer: caller.clone(),
        };
        self.assets.insert(id.clone(), asset.clone());

        let balance = Balance {
            current: payload.supply,
            locked: 0,
        };
        self.sdk.set_account_value(&caller, id, balance);

        let event_string = serde_json_string!(asset);
        ctx.emit_event("CreateAsset".to_owned(), event_string);

        ServiceResponse::from_succeed(asset)
    }

    #[cycles(100_00)]
    #[read]
    fn get_asset(&self, ctx: ServiceContext, payload: GetAssetPayload) -> ServiceResponse<Asset> {
        match self.assets.get(&payload.id) {
            Some(asset) => ServiceResponse::from_succeed(asset),
            None => AssetError::AssetExisted { id: payload.id }.into(),
        }
    }

    #[cycles(100_00)]
    #[read]
    fn get_balance(
        &self,
        ctx: ServiceContext,
        payload: GetBalancePayload,
    ) -> ServiceResponse<GetBalanceResponse> {
        let balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)
            .unwrap_or(Balance::default());
        ServiceResponse::from_succeed(GetBalanceResponse {
            asset_id: payload.asset_id,
            balance,
        })
    }

    #[cycles(210_00)]
    #[write]
    fn transfer(&mut self, ctx: ServiceContext, payload: TransferPayload) -> ServiceResponse<()> {
        let sub_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: ctx.get_caller(),
            value: payload.value,
        };
        call_and_parse_service_response!(self, _sub_value, &sub_payload);

        let add_payload = ModifyBalancePayload {
            asset_id: payload.asset_id.clone(),
            user: payload.to.clone(),
            value: payload.value,
        };
        call_and_parse_service_response!(self, _add_value, &add_payload);

        let event = TransferEvent {
            asset_id: payload.asset_id,
            from: ctx.get_caller(),
            to: payload.to,
            value: payload.value,
        };
        let event_json = serde_json_string!(event);
        ctx.emit_event("TransferAsset".to_owned(), event_json);
        ServiceResponse::from_succeed(())
    }

    fn _add_value(&mut self, payload: &ModifyBalancePayload) -> ServiceResponse<()> {
        if !self.assets.contains(&payload.asset_id) {
            return AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into();
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)
            .unwrap_or(Balance::default());

        let (result, overflow) = balance.current.overflowing_add(payload.value);
        if overflow {
            return AssetError::U64Overflow.into();
        }

        balance.current = result;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id.clone(), balance);
        ServiceResponse::from_succeed(())
    }

    fn _sub_value(&mut self, payload: &ModifyBalancePayload) -> ServiceResponse<()> {
        if !self.assets.contains(&payload.asset_id) {
            return AssetError::AssetNotExist {
                id: payload.asset_id.clone(),
            }
            .into();
        }

        let mut balance: Balance = self
            .sdk
            .get_account_value(&payload.user, &payload.asset_id)
            .unwrap_or(Balance::default());

        if balance.current < payload.value {
            return AssetError::InsufficientBalance {
                wanted: payload.value,
                had: balance.current,
            }
            .into();
        }

        balance.current = balance.current - payload.value;
        self.sdk
            .set_account_value(&payload.user, payload.asset_id.clone(), balance);
        ServiceResponse::from_succeed(())
    }
}

#[derive(Debug, Display)]
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

impl AssetError {
    fn code(&self) -> u64 {
        match self {
            AssetError::JsonParse(_) => 101,
            AssetError::AssetExisted { .. } => 102,
            AssetError::AssetNotExist { .. } => 103,
            AssetError::InsufficientBalance { .. } => 104,
            AssetError::U64Overflow => 105,
            AssetError::PermissionDenial => 106,
        }
    }
}

impl<T: Default> From<AssetError> for ServiceResponse<T> {
    fn from(err: AssetError) -> ServiceResponse<T> {
        ServiceResponse::from_error(err.code(), err.to_string())
    }
}
