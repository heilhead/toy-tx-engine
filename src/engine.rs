use crate::account::{AccountData, AccountStore, BalanceOperation};
use crate::input::InputStreamError;
use crate::transaction::{
    RawTransactionData, StoredTransactionType, TransactionStatus, TransactionStore,
    TransactionStoreError, TransactionType,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum EngineError {
    #[error(transparent)]
    DecodingError(#[from] InputStreamError),

    #[error(transparent)]
    InvalidTransactionData(#[from] ValidationError),

    #[error("Invalid transaction status: Required={required:?} Actual={actual:?}")]
    InvalidTransactionStatus {
        required: TransactionStatus,
        actual: TransactionStatus,
    },

    #[error("Invalid transaction type: Required={required:?} Actual={actual:?}")]
    InvalidTransactionType {
        required: StoredTransactionType,
        actual: StoredTransactionType,
    },

    #[error(transparent)]
    TransactionStoreError(#[from] TransactionStoreError),

    #[error("Internal error")]
    InternalError,
}

#[derive(Debug, ThisError)]
pub enum ValidationError {
    #[error("Account locked")]
    AccountLocked,

    #[error("Invalid account ID")]
    InvalidAccountId,

    #[error("Invalid transaction ID")]
    InvalidTransactionId,

    #[error("Invalid transaction amount")]
    InvalidAmount,

    #[error("Malformed transaction data")]
    MalformedTransactionData,
}

pub type EngineResult<T> = Result<T, EngineError>;

pub struct Engine {
    accounts: AccountStore,
    transactions: TransactionStore,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            accounts: AccountStore::new(),
            transactions: TransactionStore::new(),
        }
    }

    pub fn process_transaction(&mut self, data: &RawTransactionData) -> EngineResult<()> {
        // Perform general data validation for both the incoming transaction data and
        // the database state.
        self.validate_transaction(&data)?;

        match data.ty {
            TransactionType::Deposit | TransactionType::Withdrawal => {
                self.process_balance_operation(data)
            }

            TransactionType::Dispute => self.process_dispute(data),

            TransactionType::Resolve => self.process_resolution(data),

            TransactionType::Chargeback => self.process_chargeback(data),
        }
    }

    pub fn accounts(&self) -> impl Iterator<Item = &AccountData> {
        self.accounts.iter()
    }

    fn process_balance_operation(&mut self, data: &RawTransactionData) -> EngineResult<()> {
        // Safe guarantees at this point:
        //  - account either does not exist (create below) OR does exist and is upstanding;
        //  - transaction has a valid positive amount;
        //  - transaction ID is unique and can be inserted into the database;

        let amount = data.amount.ok_or(EngineError::InternalError)?;

        let op = match data.ty {
            TransactionType::Deposit => BalanceOperation::Deposit(amount),
            TransactionType::Withdrawal => BalanceOperation::WithdrawAvailable(amount),
            _ => return Err(EngineError::InternalError),
        };

        // For simplicity, this call initializes an account if one does not already exist.
        self.accounts.get_mut(data.account_id).update_balance(op);

        self.transactions.insert(data.try_into()?);

        Ok(())
    }

    fn process_dispute(&mut self, data: &RawTransactionData) -> EngineResult<()> {
        // Safe guarantees at this point:
        //  - account exists and is upstanding;
        //  - transaction ID is valid and transaction data exists in the database;

        let tx = self
            .transactions
            .get_mut(data.id)
            .ok_or(EngineError::InternalError)?;

        // This bit wasn't clear in the task description: should we allow only deposit transactions
        // to be reversed, or both deposit and withdrawal? As someone who's never done this before,
        // my assumption here is that we allow only deposits to be reversed, and withdrawal reversal
        // is done through a separate deposit transaction. I may be wrong here though :(
        // TODO: Cover this concern in the readme.
        if tx.ty != StoredTransactionType::Deposit {
            return Err(EngineError::InvalidTransactionType {
                required: StoredTransactionType::Deposit,
                actual: tx.ty,
            });
        }

        if tx.status != TransactionStatus::Ok {
            return Err(EngineError::InvalidTransactionStatus {
                required: TransactionStatus::Ok,
                actual: tx.status,
            });
        }

        tx.status = TransactionStatus::UnderDispute;

        self.accounts
            .get_mut(data.account_id)
            .update_balance(BalanceOperation::Hold(tx.amount));

        Ok(())
    }

    fn process_resolution(&mut self, data: &RawTransactionData) -> EngineResult<()> {
        // Safe guarantees at this point:
        //  - account exists and is upstanding;
        //  - transaction ID is valid and transaction data exists in the database;

        let tx = self
            .transactions
            .get_mut(data.id)
            .ok_or(EngineError::InternalError)?;

        if tx.status != TransactionStatus::UnderDispute {
            return Err(EngineError::InvalidTransactionStatus {
                required: TransactionStatus::UnderDispute,
                actual: tx.status,
            });
        }

        tx.status = TransactionStatus::Ok;

        self.accounts
            .get_mut(data.account_id)
            .update_balance(BalanceOperation::Release(tx.amount));

        Ok(())
    }

    fn process_chargeback(&mut self, data: &RawTransactionData) -> EngineResult<()> {
        // Safe guarantees at this point:
        //  - account exists and is upstanding;
        //  - transaction ID is valid and transaction data exists in the database;

        let tx = self
            .transactions
            .get_mut(data.id)
            .ok_or(EngineError::InternalError)?;

        if tx.status != TransactionStatus::UnderDispute {
            return Err(EngineError::InvalidTransactionStatus {
                required: TransactionStatus::UnderDispute,
                actual: tx.status,
            });
        }

        tx.status = TransactionStatus::Cancelled;

        let account = self.accounts.get_mut(data.account_id);
        account.update_balance(BalanceOperation::WithdrawHeld(tx.amount));
        account.set_locked(true);

        Ok(())
    }

    /// Performs common validations shared between multiple transaction types.
    fn validate_transaction(&self, data: &RawTransactionData) -> Result<(), ValidationError> {
        let account = self.accounts.get(data.account_id);

        // If the account does exist and it's locked, it's a no-go. If the account does not exist,
        // it'll be created later.
        if matches!(account, Some(account) if account.locked()) {
            return Err(ValidationError::AccountLocked);
        }

        match data.ty {
            TransactionType::Deposit | TransactionType::Withdrawal => {
                if self.transactions.exists(data.id) {
                    return Err(ValidationError::InvalidTransactionId);
                }

                static ZERO: Decimal = dec!(0.0);

                if let Some(amount) = &data.amount {
                    if amount > &ZERO {
                        Ok(())
                    } else {
                        Err(ValidationError::InvalidAmount)
                    }
                } else {
                    Err(ValidationError::MalformedTransactionData)
                }
            }

            TransactionType::Dispute | TransactionType::Resolve | TransactionType::Chargeback => {
                if account.is_none() {
                    return Err(ValidationError::InvalidAccountId);
                }

                if !self.transactions.exists(data.id) {
                    return Err(ValidationError::InvalidTransactionId);
                }

                if let Some(_) = &data.amount {
                    // We do not expect to have any amount for these types of transactions.
                    return Err(ValidationError::MalformedTransactionData);
                }

                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::Engine;
    use crate::account::AccountBalance;
    use crate::engine::{EngineError, ValidationError};
    use crate::transaction::RawTransactionData;
    use crate::InputStream;
    use rust_decimal_macros::dec;

    fn create_input(csv_data: &'static str) -> Vec<RawTransactionData> {
        let test_input = InputStream::from_reader(&csv_data.as_bytes()[..]).unwrap();
        test_input.map(|tx| tx.unwrap()).collect::<Vec<_>>()
    }

    #[test]
    fn transaction_types() -> anyhow::Result<()> {
        let test_input = create_input(
            r"type, client, tx, amount
            deposit, 1, 1, 15.0
            withdrawal, 1, 2, 5.0
            deposit, 1, 3, 5.0,
            dispute, 1, 3
            resolve, 1, 3
            deposit, 1, 4, 5.0,
            dispute, 1, 4
            chargeback, 1, 4",
        );

        let test_balance = vec![
            AccountBalance::with_amount(dec!(15.0), dec!(0.0)),
            AccountBalance::with_amount(dec!(10.0), dec!(0.0)),
            AccountBalance::with_amount(dec!(15.0), dec!(0.0)),
            AccountBalance::with_amount(dec!(15.0), dec!(5.0)),
            AccountBalance::with_amount(dec!(15.0), dec!(0.0)),
            AccountBalance::with_amount(dec!(20.0), dec!(0.0)),
            AccountBalance::with_amount(dec!(20.0), dec!(5.0)),
            AccountBalance::with_amount(dec!(15.0), dec!(0.0)),
        ];

        let mut engine = Engine::new();

        for (tx, balance) in std::iter::zip(test_input, test_balance) {
            engine.process_transaction(&tx)?;

            assert_eq!(engine.accounts.balance(1).unwrap(), &balance);
        }

        let account = engine.accounts.get(1).unwrap();

        assert!(account.locked());
        assert_eq!(
            account.balance(),
            &AccountBalance::with_amount(dec!(15.0), dec!(0.0))
        );

        Ok(())
    }

    #[test]
    fn transaction_errors() -> anyhow::Result<()> {
        let input = create_input(
            r"type, client, tx, amount
            deposit, 1, 1, 15.0
            dispute, 1, 2
            resolve, 1, 1
            dispute, 2, 1
            deposit, 1, 3
            deposit, 1, 4, -10.0
            deposit, 2, 5, 10.0
            dispute, 2, 5
            chargeback, 2, 5
            deposit, 2, 6, 10.0",
        );

        let mut input = input.iter();
        let mut engine = Engine::new();
        let mut next = move || engine.process_transaction(input.next().unwrap());

        next()?;

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionData(
                ValidationError::InvalidTransactionId
            ))
        ));

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionStatus { .. })
        ));

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionData(
                ValidationError::InvalidAccountId
            ))
        ));

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionData(
                ValidationError::MalformedTransactionData
            ))
        ));

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionData(
                ValidationError::InvalidAmount
            ))
        ));

        next()?;
        next()?;
        next()?;

        assert!(matches!(
            next(),
            Err(EngineError::InvalidTransactionData(
                ValidationError::AccountLocked
            ))
        ));

        Ok(())
    }
}
