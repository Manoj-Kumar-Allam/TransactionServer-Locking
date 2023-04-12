package transaction.server.transaction;

import transaction.server.TransactionServer;
import transaction.server.lock.Lock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The class [Transaction] represents a transaction with a unique ID, a list of locks that it is holding,
 * and a before-image of account balances
 *
 * @author manoj
 */
public class Transaction {

    // unique ID of the transaction
    int transactionId;

    // locks that the transaction is holding
    List<Lock> locks;

    // before-image of account balances
    Map<Integer, Integer> beforeImage;

    // log of actions performed during the transaction
    StringBuffer log = new StringBuffer("");

    /**
     * Constructs a new Transaction object with a given ID.
     *
     * @param transactionId the unique ID of the transaction
     */
    Transaction(int transactionId) {
        this.transactionId = transactionId;
        this.locks = new ArrayList<>();
        this.beforeImage = new HashMap<>();
    }

    /**
     * Gets the ID of the transaction.
     * @return the ID of the transaction
     */
    public int getTransactionId() {
        return transactionId;
    }

    /**
     * Gets the locks that the transaction is holding.
     * @return a list of Lock objects held by the transaction
     */
    public List<Lock> getLocks() {
        return locks;
    }

    /**
     * Adds a lock to the list of locks held by the transaction.
     *
     * @param lock the Lock object to be added to the transaction's list of locks
     */
    public void addLock(Lock lock) {
        this.locks.add(lock);
    }

    /**
     * Gets the before-image of account balances.
     *
     * @return a Map object representing the before-image of account balances
     */
    public Map<Integer, Integer> getBeforeImage() {
        return beforeImage;
    }

    /**
     * Adds an account and its balance to the before-image of account balances.
     *
     * @param account the account number to be added to the before-image
     * @param balance the balance to be associated with the account in the before-image
     */
    public void addBeforeImage(int account, int balance) {
        this.beforeImage.put(account, balance);
        this.log("[Transaction.addBeforeImage] " + transactionId + " | set before image for account #" +
                account + " with balance " + balance);
    }

    /**
     * Logs a message related to the transaction.
     *
     * @param logString the message to be logged
     */
    public void log(String logString) {
        int messageCount = TransactionServer.getMessageCount();

        log.append("\n").append(messageCount).append(" ").append(logString);

        if(!TransactionServer.transactionView) {
            System.out.println(messageCount + " Transaction !" + transactionId);
        }
    }

    /**
     * Gets the log of actions performed during the transaction.
     *
     * @return a StringBuffer containing the log of the transaction's actions
     */
    public StringBuffer getLog() {
        return log;
    }
}
