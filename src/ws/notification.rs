use std::{borrow::Cow, fmt::Display};

use serde::Deserialize;

use crate::types::{Account, CachedPubkey, Encoding};

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
#[serde(untagged)]
pub(crate) enum WsMessage {
    SubResult(SubResult),
    UnsubResult(UnsubResult),
    SubError(SubError),
    Notification(Notification),
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct SubResult {
    pub id: u64,
    pub result: u64,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct SubError {
    pub id: u64,
    pub error: JsonRpcError,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct JsonRpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct UnsubResult {
    pub(crate) id: u64,
    pub(crate) result: bool,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct Notification {
    pub params: NotificationParams,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub(crate) struct NotificationParams {
    pub result: NotificationResult,
    pub subscription: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub enum NotificationValue {
    Account(AccountNotification),
    Program(ProgramNotification),
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct Context {
    pub slot: u64,
}

#[derive(Deserialize)]
#[serde(untagged)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub enum NotificationResult {
    Account(AccountResult),
    Slot(SlotResult),
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct AccountResult {
    pub context: Context,
    pub value: NotificationValue,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct SlotResult {
    pub parent: u64,
    pub root: u64,
    pub slot: u64,
}

#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct AccountData(pub Vec<u8>);

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct AccountNotification {
    pub data: AccountData,
    pub executable: bool,
    pub lamports: u64,
    pub owner: CachedPubkey,
    #[serde(rename = "rentEpoch")]
    pub rent_epoch: u64,
}

#[derive(Deserialize)]
#[cfg_attr(test, derive(Debug, PartialEq, Eq))]
pub struct ProgramNotification {
    pub pubkey: CachedPubkey,
    pub account: AccountNotification,
}

impl Display for JsonRpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "code: {}, message: {}", self.code, self.message)
    }
}

impl TryFrom<&str> for WsMessage {
    type Error = json::Error;

    #[inline]
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        json::from_str(value)
    }
}

impl From<AccountNotification> for Account {
    fn from(notification: AccountNotification) -> Self {
        Self {
            owner: notification.owner,
            data: notification.data.0,
            executable: notification.executable,
            rent_epoch: notification.rent_epoch,
            lamports: notification.lamports,
        }
    }
}

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

#[test]
fn test_sub_result() {
    let msg = r#"{ "jsonrpc": "2.0", "result": 23784, "id": 1 }"#;
    let wsmsg: WsMessage = json::from_str(msg).unwrap();

    assert_eq!(
        wsmsg,
        WsMessage::SubResult(SubResult {
            result: 23784,
            id: 1
        })
    );
}

#[test]
fn test_unsub_result() {
    let msg = r#"{ "jsonrpc": "2.0", "result": false, "id": 1 }"#;
    let wsmsg: WsMessage = json::from_str(msg).unwrap();

    assert_eq!(
        wsmsg,
        WsMessage::UnsubResult(UnsubResult {
            result: false,
            id: 1
        })
    );
}

#[test]
fn test_account_notification() {
    let msg = r#"{
      "jsonrpc": "2.0",
      "method": "accountNotification",
      "params": {
        "result": {
          "context": {
            "slot": 5199307
          },
          "value": {
            "data": [
              "11116bv5nS2h3y12kD1yUKeMZvGcKLSjQgX6BeV7u1FrjeJcKfsHPXHRDEHrBesJhZyqnnq9qJeUuF7WHxiuLuL5twc38w2TXNLxnDbjmuR",
              "base58"
            ],
            "executable": false,
            "lamports": 33594,
            "owner": "11111111111111111111111111111111",
            "rentEpoch": 635
          }
        },
        "subscription": 23784
      }
    }"#;
    let wsmsg: WsMessage = json::from_str(msg).unwrap();
    assert_eq!(
        wsmsg,
        WsMessage::Notification(Notification {
            params: NotificationParams {
                result: NotificationResult::Account(AccountResult {
                    context: Context { slot: 5199307 },
                    value: NotificationValue::Account(AccountNotification {
                        data: AccountData(vec![
                            0, 0, 0, 0, 1, 0, 0, 0, 2, 183, 51, 108, 200, 154, 214, 210, 230, 171,
                            188, 243, 224, 56, 167, 48, 211, 116, 164, 157, 73, 180, 183, 106, 32,
                            147, 212, 195, 118, 43, 24, 44, 4, 253, 55, 48, 180, 221, 13, 242, 20,
                            10, 23, 137, 230, 76, 108, 164, 178, 14, 63, 41, 25, 197, 109, 243,
                            145, 199, 255, 14, 174, 134, 91, 165, 136, 19, 0, 0, 0, 0, 0, 0
                        ]),
                        executable: false,
                        lamports: 33594,
                        owner: CachedPubkey::new([0; 32]),
                        rent_epoch: 635,
                    }),
                }),
                subscription: 23784,
            },
        })
    );
}

#[test]
fn test_program_notification() {
    let msg = r#"{
      "jsonrpc": "2.0",
      "method": "programNotification",
      "params": {
        "result": {
          "context": {
            "slot": 5208469
          },
          "value": {
            "pubkey": "H4vnBqifaSACnKa7acsxstsY1iV1bvJNxsCY7enrd1hq",
            "account": {
              "data": [
                "11116bv5nS2h3y12kD1yUKeMZvGcKLSjQgX6BeV7u1FrjeJcKfsHPXHRDEHrBesJhZyqnnq9qJeUuF7WHxiuLuL5twc38w2TXNLxnDbjmuR",
                "base58"
              ],
              "executable": false,
              "lamports": 33594,
              "owner": "11111111111111111111111111111111",
              "rentEpoch": 636
            }
          }
        },
        "subscription": 24040
      }
    }"#;
    let wsmsg: WsMessage = json::from_str(msg).unwrap();

    assert_eq!(
        wsmsg,
        WsMessage::Notification(Notification {
            params: NotificationParams {
                result: NotificationResult::Account(AccountResult {
                    context: Context { slot: 5208469 },
                    value: NotificationValue::Program(ProgramNotification {
                        pubkey: CachedPubkey::new([
                            238, 188, 138, 156, 177, 213, 119, 129, 86, 185, 133, 155, 23, 5, 198,
                            165, 73, 217, 197, 181, 191, 87, 39, 178, 98, 175, 172, 92, 133, 90,
                            215, 80
                        ]),
                        account: AccountNotification {
                            data: AccountData(vec![
                                0, 0, 0, 0, 1, 0, 0, 0, 2, 183, 51, 108, 200, 154, 214, 210, 230,
                                171, 188, 243, 224, 56, 167, 48, 211, 116, 164, 157, 73, 180, 183,
                                106, 32, 147, 212, 195, 118, 43, 24, 44, 4, 253, 55, 48, 180, 221,
                                13, 242, 20, 10, 23, 137, 230, 76, 108, 164, 178, 14, 63, 41, 25,
                                197, 109, 243, 145, 199, 255, 14, 174, 134, 91, 165, 136, 19, 0, 0,
                                0, 0, 0, 0
                            ]),
                            executable: false,
                            lamports: 33594,
                            owner: CachedPubkey::new([0; 32]),
                            rent_epoch: 636,
                        }
                    }),
                }),
                subscription: 24040,
            },
        })
    );
}
