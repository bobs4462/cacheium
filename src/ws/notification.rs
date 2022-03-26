use std::borrow::Cow;

use serde::Deserialize;

use crate::{Encoding, Pubkey};

pub struct Notification {
    method: NotificationMethod,
    params: (),
    subscription: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NotificationMethod {
    AccountNotification,
    ProgramNotification,
}

pub struct NotificationParams {
    context: Context,
    value: AccountNotification,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum NotificationValue {
    Account(AccountNotification),
    Program(ProgramNotification),
}

pub struct Context {
    slot: u64,
}

#[derive(Deserialize)]
pub struct AccountNotification {
    data: AccountData,
    executable: bool,
    lamports: u64,
    owner: Pubkey,
    rent_epoch: u64,
}

#[derive(Deserialize)]
pub struct ProgramNotification {
    pubkey: Pubkey,
    account: AccountNotification,
}

pub struct AccountData(Vec<u8>);

impl<'de> Deserialize<'de> for AccountData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = AccountData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("base58 encoded string")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use serde::de::Error as DeError;
                let data: Cow<'_, str> = seq
                    .next_element()?
                    .ok_or_else(|| DeError::custom("no account data provided"))?;
                let encoding: Encoding = seq
                    .next_element()?
                    .ok_or_else(|| DeError::custom("no data encoding provided"))?;
                let data = match encoding {
                    Encoding::Base58 => bs58::decode(data.as_bytes())
                        .into_vec()
                        .map_err(DeError::custom)?,
                    Encoding::Base64 => base64::decode(data.as_bytes()).map_err(DeError::custom)?,
                    Encoding::Base64Zstd => base64::decode(data.as_bytes())
                        .map(|data| zstd::decode_all(data.as_slice()))
                        .map_err(DeError::custom)?
                        .map_err(DeError::custom)?,
                };
                Ok(AccountData(data))
            }
        }

        deserializer.deserialize_seq(Visitor)
    }
}
