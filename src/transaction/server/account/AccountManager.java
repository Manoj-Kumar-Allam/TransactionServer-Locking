package transaction.server.account;

import transaction.server.TransactionServer;
import transaction.server.lock.LockType;
import transaction.server.lock.TransactionAbortedException;
import transaction.server.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * Manages bank accounts by providing operations to read and write balances.
 *
 * @author manoj and srinivas
 */
public class AccountManager {

    // Map of account numbers to Account objects
    private final Map<Integer, Account> accounts;

    /**
     * Creates an empty account manager.
     */
    public AccountManager() {
        accounts = new HashMap<>();
    }

    /**
     * Creates a new account manager with the specified number of accounts and initial balance.
     *
     * @param numberOfAccounts the number of accounts to create
     * @param initialBalance the initial balance for each account
     */
    public AccountManager(int numberOfAccounts, int initialBalance) {
        accounts = new HashMap<>(numberOfAccounts);

        // Create the specified number of accounts with the given initial balance
        for (int i = 1; i <= numberOfAccounts; i++) {
            accounts.put(i, new Account(i, initialBalance));
        }
    }

    /**
     * Returns the balance of the account with the specified account number.
     *
     * @param accountNumber the account number
     * @param transaction the transaction requesting the read operation
     * @return the balance of the account, or null if the account doesn't exist
     * @throws TransactionAbortedException if the transaction was aborted due to conflicts with other transactions
     */
    public Integer read(int accountNumber, Transaction transaction) throws TransactionAbortedException {
        Account account = accounts.get(accountNumber);

        if (account != null) {
            TransactionServer.lockManager.setLock(account, transaction, LockType.READ_LOCK);

            transaction.addBeforeImage(account.getAccountNumber(), account.getBalance());

            return account.getBalance();
        } else {
            System.err.println("Invalid Account; Account# " + accountNumber + " doesn't exist");
            return null;
        }
    }

    /**
     * Sets the balance of the account with the specified account number.
     *
     * @param accountNumber the account number
     * @param balance the new balance
     * @param transaction the transaction requesting the write operation
     * @return true if the balance was set successfully, or false if the account doesn't exist
     * @throws TransactionAbortedException if the transaction was aborted due to conflicts with other transactions
     */
    public boolean write(int accountNumber, int balance, Transaction transaction) throws TransactionAbortedException {
        // Look up the account by its account number
        Account account = accounts.get(accountNumber);

        if (account != null) {
            // Acquire write lock on the account for the requesting transaction
            TransactionServer.lockManager.setLock(account, transaction, LockType.WRITE_LOCK);

            // Record the account's state before the write operation for use in the transaction's undo log
            transaction.addBeforeImage(account.getAccountNumber(), account.getBalance());

            // Update the account's balance
            account.setBalance(balance);

            return true;
        } else {
            System.err.println("Invalid Account; Account# " + accountNumber + " doesn't exist");
            return false;
        }
    }

    public int _read(int accountNumber) {
        Account account = accounts.get(accountNumber);

        if(account != null) {
            return account.getBalance();
        }
        return 0;
    }

    /**
     * Returns the account with the specified account number.
     *
     * @param accountNumber the account number
     * @return the Account object associated with the account number, or null if the account doesn't exist
     */
    public Account getAccountByAccountNumber(int accountNumber) {
        Account account = accounts.get(accountNumber);
        return account != null ? account : null;
    }
}