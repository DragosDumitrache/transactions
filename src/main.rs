use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Error;

use csv::Trim;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Transaction {
    #[serde(rename(deserialize = "type"))]
    transaction_type: TransactionType,
    client: u16,
    tx: u32,
    amount: Option<f32>,
}

#[derive(Clone)]
struct Account {
    disputed_transactions: Vec<u32>,
    frozen: bool,
    held: f32,
    available: f32,
}

impl Account {
    fn deposit(&mut self, amount: f32) {
        // Assuming that if the account is frozen, all deposit/withdrawal operations are blocked.
        if !self.frozen {
            self.available += amount;
        }
    }

    fn withdraw(&mut self, amount: f32) {
        // Assuming that if the account is frozen, all deposit/withdrawal operations are blocked.
        if amount > self.available && !self.frozen {
            return;
        }
        self.available -= amount;
    }

    fn dispute(&mut self, transaction_id: u32, amount: f32) {
        self.disputed_transactions.push(transaction_id);
        self.held += amount;
    }

    fn resolve(&mut self, transaction_id: u32, amount: f32) {
        if self.disputed_transactions.contains(&transaction_id) {
            self.disputed_transactions.retain(|x| x != &transaction_id);
            self.held -= amount;
            self.available += amount;
        }
    }

    fn chargeback(&mut self, transaction_id: u32, amount: f32) {
        if self.disputed_transactions.contains(&transaction_id) {
            self.disputed_transactions.retain(|x| x != &transaction_id);
            self.held -= amount;
            self.frozen = true;
        }
    }
    fn total_funds(&self) -> f32 {
        self.available + self.held
    }
}

fn read_csv_file(filename: &str) -> std::io::Result<Vec<Transaction>> {
    let file = File::open(filename)?;
    let mut rdr = csv::ReaderBuilder::new().trim(Trim::All).from_reader(file);
    Ok(rdr.deserialize()
        .into_iter()
        .map(|result| {
            result.unwrap()
        })
        .collect())
}

fn process_transactions(transactions: Vec<Transaction>) -> HashMap<u16, Account> {
    let mut accounts: HashMap<u16, Account> = HashMap::new();
    let mut processed_transactions: HashMap<u32, Transaction> = HashMap::new();

    for transaction in transactions.into_iter() {
        let client_id = transaction.client;
        let user_account = accounts.entry(client_id).or_insert(Account {
            disputed_transactions: vec![],
            frozen: false,
            held: 0.0,
            available: 0.0,
        });

        match transaction.transaction_type {
            TransactionType::Deposit => {
                user_account.deposit(transaction.amount.unwrap());
                processed_transactions.insert(transaction.tx, transaction);
            }
            TransactionType::Withdrawal => {
                user_account.withdraw(transaction.amount.unwrap());
                processed_transactions.insert(transaction.tx, transaction);
            }
            TransactionType::Dispute => {
                let possible_disputed_transaction = processed_transactions.get(&transaction.tx);
                match possible_disputed_transaction {
                    Some(disputed_transaction)
                    if disputed_transaction.transaction_type == TransactionType::Deposit
                        || disputed_transaction.transaction_type == TransactionType::Withdrawal =>
                        {
                            user_account.dispute(
                                disputed_transaction.tx,
                                disputed_transaction.amount.unwrap(),
                            )
                        }
                    _ => {}
                }
            }
            TransactionType::Resolve => {
                let possible_transaction = processed_transactions.get(&transaction.tx);
                match possible_transaction {
                    Some(disputed_transaction)
                    if disputed_transaction.transaction_type == TransactionType::Deposit
                        || disputed_transaction.transaction_type
                        == TransactionType::Withdrawal =>
                        {
                            user_account.resolve(
                                disputed_transaction.tx,
                                disputed_transaction.amount.unwrap(),
                            )
                        }
                    _ => {}
                }
            }
            TransactionType::Chargeback => {
                let possible_transaction = processed_transactions.get(&transaction.tx);
                match possible_transaction {
                    Some(disputed_transaction)
                    if disputed_transaction.transaction_type == TransactionType::Deposit
                        || disputed_transaction.transaction_type
                        == TransactionType::Withdrawal =>
                        {
                            user_account.chargeback(
                                disputed_transaction.tx,
                                disputed_transaction.amount.unwrap(),
                            )
                        }
                    _ => {}
                }
            }
        }
    }
    return accounts;
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    let filename = &args[1];
    let transactions = read_csv_file(filename)?;
    let accounts = process_transactions(transactions);
    println!("client, available, held, total, locked");
    for (client, account) in accounts.iter() {
        println!("{}, {}, {}, {}, {}", client, account.available, account.held, account.total_funds(), account.frozen);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deposit_gets_processed_successfully() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(10.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 1,
            amount: Some(20.0),
        };
        let accounts = process_transactions(vec![t1, t2]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.available, 30.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 30.0);
    }

    #[test]
    fn withdrawal_is_ignored_if_insufficient_funds() {
        let t1 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 0,
            amount: Some(10.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(20.0),
        };
        let accounts = process_transactions(vec![t1, t2]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.available, 0.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 0.0);
    }

    #[test]
    fn withdrawal_is_ignored_once_amount_exceeds_funds() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(10.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 2,
            amount: Some(12.0),
        };
        let accounts = process_transactions(vec![t1, t2, t3]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.available, 10.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 10.0);
    }

    #[test]
    fn disputing_a_real_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![1]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 5.0);
        assert_eq!(user_0_account.total_funds(), 20.0);
    }

    #[test]
    fn disputing_a_fake_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 2,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 15.0);
    }

    #[test]
    fn resolving_a_disputed_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let t4 = Transaction {
            transaction_type: TransactionType::Resolve,
            client: 0,
            tx: 1,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3, t4]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![]);
        assert_eq!(user_0_account.available, 20.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 20.0);
    }

    #[test]
    fn resolving_a_fake_disputed_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let t4 = Transaction {
            transaction_type: TransactionType::Resolve,
            client: 0,
            tx: 2,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3, t4]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![1]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 5.0);
        assert_eq!(user_0_account.total_funds(), 20.0);
    }

    #[test]
    fn chargeback_a_disputed_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let t4 = Transaction {
            transaction_type: TransactionType::Chargeback,
            client: 0,
            tx: 1,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3, t4]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 0.0);
        assert_eq!(user_0_account.total_funds(), 15.0);
    }

    #[test]
    fn chargeback_an_existing_non_disputed_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let t4 = Transaction {
            transaction_type: TransactionType::Chargeback,
            client: 0,
            tx: 0,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3, t4]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![1]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 5.0);
        assert_eq!(user_0_account.total_funds(), 20.0);
    }

    #[test]
    fn chargeback_a_non_existing_disputed_transaction() {
        let t1 = Transaction {
            transaction_type: TransactionType::Deposit,
            client: 0,
            tx: 0,
            amount: Some(20.0),
        };
        let t2 = Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 0,
            tx: 1,
            amount: Some(5.0),
        };
        let t3 = Transaction {
            transaction_type: TransactionType::Dispute,
            client: 0,
            tx: 1,
            amount: None,
        };
        let t4 = Transaction {
            transaction_type: TransactionType::Chargeback,
            client: 0,
            tx: 5,
            amount: None,
        };
        let accounts = process_transactions(vec![t1, t2, t3, t4]);
        assert!(accounts.contains_key(&0));

        let user_0_account = accounts.get(&0).unwrap();
        assert_eq!(user_0_account.disputed_transactions, vec![1]);
        assert_eq!(user_0_account.available, 15.0);
        assert_eq!(user_0_account.held, 5.0);
        assert_eq!(user_0_account.total_funds(), 20.0);
    }

    #[test]
    fn read_non_existent_csv_file() {
        assert!(read_csv_file("NoSuchFile").is_err());
    }

    #[test]
    fn read_existent_csv_file() {
        assert!(read_csv_file("transaction.csv").is_ok());
    }

    #[test]
    fn ensure_parsed_transactions_are_correct() {
        let parsed_transactions = read_csv_file("test.csv");
        assert!(parsed_transactions.is_ok());
        let transactions = parsed_transactions.unwrap();
        assert_eq!(transactions.len(), 6);
        assert_eq!(transactions[0], Transaction {
            transaction_type: TransactionType::Deposit,
            client: 1,
            tx: 1,
            amount: Some(1.0),
        });

        assert_eq!(transactions[1], Transaction {
            transaction_type: TransactionType::Withdrawal,
            client: 2,
            tx: 2,
            amount: Some(2.0),
        });

        assert_eq!(transactions[2], Transaction {
            transaction_type: TransactionType::Dispute,
            client: 1,
            tx: 1,
            amount: None,
        });

        assert_eq!(transactions[3], Transaction {
            transaction_type: TransactionType::Resolve,
            client: 1,
            tx: 4,
            amount: None,
        });

        assert_eq!(transactions[4], Transaction {
            transaction_type: TransactionType::Dispute,
            client: 2,
            tx: 2,
            amount: None,
        });

        assert_eq!(transactions[5], Transaction {
            transaction_type: TransactionType::Chargeback,
            client: 2,
            tx: 2,
            amount: None,
        });
    }
}
