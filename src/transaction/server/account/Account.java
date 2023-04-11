package transaction.server.account;

/**
 * Represents a bank account with an account number and balance.
 *
 * @author srinivas
 */
public class Account {

    private final int accountNumber;
    private int balance;

    /**
     * Creates a new account with the specified account number and balance.
     *
     * @param accountNumber the account number
     * @param balance the initial balance
     */
    public Account(int accountNumber, int balance) {
        this.accountNumber = accountNumber;
        this.balance = balance;
    }

    /**
     * Returns the account number.
     *
     * @return the account number
     */
    public int getAccountNumber() {
        return accountNumber;
    }

    /**
     * Returns the balance.
     *
     * @return the balance
     */
    public int getBalance() {
        return balance;
    }

    /**
     * Sets the balance.
     *
     * @param balance the new balance
     */
    public void setBalance(int balance) {
        this.balance = balance;
    }
}