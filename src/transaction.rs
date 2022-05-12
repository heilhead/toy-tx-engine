use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum TransactionStoreError {
    #[error("Transaction can not be stored: {0:?}")]
    InvalidType(TransactionType),

    #[error("Transaction amount not available")]
    AmountNotAvailable,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

/// Raw transaction data coming from input stream.
///
/// NOTE: Needs validation before executing.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct RawTransactionData {
    #[serde(rename = "tx")]
    pub id: u32,

    #[serde(rename = "type")]
    pub ty: TransactionType,

    #[serde(rename = "client")]
    pub account_id: u16,

    pub amount: Option<Decimal>,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum TransactionStatus {
    Ok,
    UnderDispute,
    Cancelled,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum StoredTransactionType {
    Deposit,
    Withdrawal,
}

impl TryFrom<TransactionType> for StoredTransactionType {
    type Error = TransactionStoreError;

    fn try_from(value: TransactionType) -> Result<Self, Self::Error> {
        match value {
            TransactionType::Deposit => Ok(Self::Deposit),
            TransactionType::Withdrawal => Ok(Self::Withdrawal),
            value => Err(TransactionStoreError::InvalidType(value)),
        }
    }
}

/// Processed transaction data, as stored in the database.
pub struct TransactionData {
    pub id: u32,
    pub ty: StoredTransactionType,
    pub account_id: u16,
    pub amount: Decimal,
    pub status: TransactionStatus,
}

impl TryFrom<&RawTransactionData> for TransactionData {
    type Error = TransactionStoreError;

    fn try_from(value: &RawTransactionData) -> Result<Self, Self::Error> {
        Ok(Self {
            id: value.id,
            ty: value.ty.try_into()?,
            account_id: value.account_id,
            amount: value
                .amount
                .ok_or(TransactionStoreError::AmountNotAvailable)?,
            status: TransactionStatus::Ok,
        })
    }
}

pub struct TransactionStore {
    data: HashMap<u32, TransactionData>,
}

#[allow(dead_code)]
impl TransactionStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    #[inline]
    pub fn exists(&self, id: u32) -> bool {
        self.data.contains_key(&id)
    }

    #[inline]
    pub fn get(&self, id: u32) -> Option<&TransactionData> {
        self.data.get(&id)
    }

    #[inline]
    pub fn get_mut(&mut self, id: u32) -> Option<&mut TransactionData> {
        self.data.get_mut(&id)
    }

    #[inline]
    pub fn insert(&mut self, data: TransactionData) {
        self.data.insert(data.id, data);
    }
}
