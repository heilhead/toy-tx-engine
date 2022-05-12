use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;

/// Represents an atomic account balance operation.
pub enum BalanceOperation {
    Deposit(Decimal),
    WithdrawAvailable(Decimal),
    Hold(Decimal),
    Release(Decimal),
    WithdrawHeld(Decimal),
}

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct AccountBalance {
    available: Decimal,
    held: Decimal,
    total: Decimal,
}

#[allow(dead_code)]
impl AccountBalance {
    pub fn with_amount(total: Decimal, held: Decimal) -> Self {
        let mut balance = Self {
            available: dec!(0.0),
            total,
            held,
        };
        balance.update_available();
        balance
    }

    /// Executes a balance operation atomically.
    pub fn update(&mut self, op: BalanceOperation) {
        match op {
            BalanceOperation::Deposit(amount) => {
                self.total += amount;
            }

            BalanceOperation::WithdrawAvailable(amount) => {
                self.total -= amount;
            }

            BalanceOperation::WithdrawHeld(amount) => {
                self.held -= amount;
                self.total -= amount;
            }

            BalanceOperation::Hold(amount) => {
                self.held += amount;
            }

            BalanceOperation::Release(amount) => {
                self.held -= amount;
            }
        }

        self.update_available();
    }

    #[inline]
    fn update_available(&mut self) {
        self.available = self.total - self.held;
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
}

#[derive(Debug, Eq, PartialEq)]
pub struct AccountData {
    id: u16,
    balance: AccountBalance,
    locked: bool,
}

#[allow(dead_code)]
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
    pub fn update_balance(&mut self, op: BalanceOperation) {
        self.balance.update(op);
    }

    #[inline]
    pub fn amount_available(&self, amount: Decimal) -> bool {
        self.balance.available >= amount
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
}

pub struct AccountStore {
    data: HashMap<u16, AccountData>,
}

#[allow(dead_code)]
impl AccountStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    #[inline]
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
    use rust_decimal_macros::dec;

    #[test]
    fn balance_ops() {
        let mut balance = AccountBalance::default();

        balance.update(BalanceOperation::Deposit(dec!(1.5)));

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(1.5),
                held: dec!(0.0),
                total: dec!(1.5)
            }
        );

        balance.update(BalanceOperation::Hold(dec!(0.75)));

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.75),
                held: dec!(0.75),
                total: dec!(1.5)
            }
        );

        balance.update(BalanceOperation::WithdrawHeld(dec!(0.05)));

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.75),
                held: dec!(0.70),
                total: dec!(1.45)
            }
        );

        balance.update(BalanceOperation::Release(dec!(0.2)));

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.95),
                held: dec!(0.50),
                total: dec!(1.45)
            }
        );

        balance.update(BalanceOperation::WithdrawAvailable(dec!(0.95)));

        assert_eq!(
            balance,
            AccountBalance {
                available: dec!(0.0),
                held: dec!(0.50),
                total: dec!(0.5)
            }
        );
    }
}
