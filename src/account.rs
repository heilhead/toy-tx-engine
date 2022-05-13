use rust_decimal::Decimal;
use std::collections::HashMap;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError, Eq, PartialEq)]
pub enum BalanceOperationError {
    #[error("Insufficient available funds: Requested={requested} Available={available}")]
    InsufficientAvailableFunds {
        requested: Decimal,
        available: Decimal,
    },

    #[error("Insufficient held funds: Requested={requested} Available={available}")]
    InsufficientHeldFunds {
        requested: Decimal,
        available: Decimal,
    },
}

/// Represents an atomic account balance operation.
pub enum BalanceOperation {
    Deposit(Decimal),
    WithdrawAvailable(Decimal),
    Hold(Decimal),
    Release(Decimal),
    WithdrawHeld(Decimal),
}

/// Client account balance representation.
///
/// Provides interface for updating balance with common transaction operations, returning errors
/// in case of invalid balance during an operation.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct AccountBalance {
    available: Decimal,
    held: Decimal,
    total: Decimal,
}

#[allow(dead_code)]
impl AccountBalance {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_amount(total: Decimal, held: Decimal) -> Result<Self, BalanceOperationError> {
        let mut balance = Self::new();
        balance.update(BalanceOperation::Deposit(total))?;
        balance.update(BalanceOperation::Hold(held))?;
        Ok(balance)
    }

    /// Executes a balance operation atomically.
    pub fn update(&mut self, op: BalanceOperation) -> Result<(), BalanceOperationError> {
        match op {
            BalanceOperation::Deposit(amount) => {
                self.total += amount;
            }

            BalanceOperation::WithdrawAvailable(amount) => {
                self.validate_available_amount(amount)?;
                self.total -= amount;
            }

            BalanceOperation::WithdrawHeld(amount) => {
                self.validate_held_amount(amount)?;
                self.held -= amount;
                self.total -= amount;
            }

            BalanceOperation::Hold(amount) => {
                self.validate_available_amount(amount)?;
                self.held += amount;
            }

            BalanceOperation::Release(amount) => {
                self.validate_held_amount(amount)?;
                self.held -= amount;
            }
        }

        self.available = self.total - self.held;

        Ok(())
    }

    #[inline]
    pub fn available(&self) -> Decimal {
        self.available
    }

    #[inline]
    pub fn held(&self) -> Decimal {
        self.held
    }

    #[inline]
    pub fn total(&self) -> Decimal {
        self.total
    }

    #[inline]
    fn validate_available_amount(&self, amount: Decimal) -> Result<(), BalanceOperationError> {
        if self.available < amount {
            Err(BalanceOperationError::InsufficientAvailableFunds {
                requested: amount,
                available: self.available,
            })
        } else {
            Ok(())
        }
    }

    #[inline]
    fn validate_held_amount(&self, amount: Decimal) -> Result<(), BalanceOperationError> {
        if self.held < amount {
            Err(BalanceOperationError::InsufficientHeldFunds {
                requested: amount,
                available: self.held,
            })
        } else {
            Ok(())
        }
    }
}

/// Client account data.
///
/// Provides an interface to account balance. Not directly serializable (at least not into CSV),
/// due to nested balance structure.
#[derive(Debug, Eq, PartialEq)]
pub struct AccountData {
    id: u16,
    balance: AccountBalance,
    locked: bool,
}

impl AccountData {
    pub fn new(id: u16) -> Self {
        Self {
            id,
            balance: Default::default(),
            locked: false,
        }
    }

    #[inline]
    pub fn id(&self) -> u16 {
        self.id
    }

    #[inline]
    pub fn set_locked(&mut self, locked: bool) {
        self.locked = locked;
    }

    #[inline]
    pub fn locked(&self) -> bool {
        self.locked
    }

    #[inline]
    pub fn balance(&self) -> &AccountBalance {
        &self.balance
    }

    #[inline]
    pub fn balance_mut(&mut self) -> &mut AccountBalance {
        &mut self.balance
    }
}

/// Account database.
///
/// A thin wrapper around a hashmap data storage.
pub struct AccountStore {
    data: HashMap<u16, AccountData>,
}

impl AccountStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn exists(&self, id: u16) -> bool {
        self.data.contains_key(&id)
    }

    #[inline]
    pub fn get(&self, id: u16) -> Option<&AccountData> {
        self.data.get(&id)
    }

    #[inline]
    pub fn get_mut(&mut self, id: u16) -> &mut AccountData {
        self.data.entry(id).or_insert_with(|| AccountData::new(id))
    }

    #[inline]
    #[allow(dead_code)]
    pub fn balance(&self, id: u16) -> Option<&AccountBalance> {
        self.get(id).map(|account| &account.balance)
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &AccountData> {
        self.data.iter().map(|(_, data)| data)
    }
}

#[cfg(test)]
mod test {
    use super::AccountBalance;
    use super::BalanceOperation;
    use crate::account::BalanceOperationError;
    use rust_decimal_macros::dec;

    #[test]
    fn balance_op_types() -> anyhow::Result<()> {
        let mut balance = AccountBalance::default();

        balance.update(BalanceOperation::Deposit(dec!(1.5)))?;

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(1.5),
                held: dec!(0.0),
                total: dec!(1.5)
            }
        );

        balance.update(BalanceOperation::Hold(dec!(0.75)))?;

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.75),
                held: dec!(0.75),
                total: dec!(1.5)
            }
        );

        balance.update(BalanceOperation::WithdrawHeld(dec!(0.05)))?;

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.75),
                held: dec!(0.70),
                total: dec!(1.45)
            }
        );

        balance.update(BalanceOperation::Release(dec!(0.2)))?;

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.95),
                held: dec!(0.50),
                total: dec!(1.45)
            }
        );

        balance.update(BalanceOperation::WithdrawAvailable(dec!(0.95)))?;

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.0),
                held: dec!(0.50),
                total: dec!(0.5)
            }
        );

        Ok(())
    }

    #[test]
    fn balance_op_errors() -> anyhow::Result<()> {
        let mut balance = AccountBalance::with_amount(dec!(10.0), dec!(5.0))?;

        assert!(matches!(
            balance.update(BalanceOperation::WithdrawAvailable(dec!(15.0))),
            Err(BalanceOperationError::InsufficientAvailableFunds { .. })
        ));

        assert!(matches!(
            balance.update(BalanceOperation::WithdrawHeld(dec!(15.0))),
            Err(BalanceOperationError::InsufficientHeldFunds { .. })
        ));

        Ok(())
    }
}
