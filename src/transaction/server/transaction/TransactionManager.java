package transaction.server.transaction;

import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * The TransactionManager class manages transactions.
 *
 * @author sampath
 */
public class TransactionManager {

    // counter for transaction IDs
    static int transactionIdCounter = 0;

    // list of transactions
    static final List<Transaction> runningTransactions = new ArrayList<>();
    static final List<Transaction> abortedTransactions = new ArrayList<>();
    static final List<Transaction> committedTransactions = new ArrayList<>();

    /**
     * Default constructor for TransactionManager class.
     */
    public TransactionManager() {}

    /**
     * Get a list of currently running transactions.
     *
     * @return A list of running transactions.
     */
    public static List<Transaction> getRunningTransactions() {
        return runningTransactions;
    }

    /**
     * Get a list of committed transactions.
     *
     * @return A list of committed transactions.
     */
    public static List<Transaction> getCommittedTransactions() {
        return committedTransactions;
    }

    /**
     * Get a list of aborted transactions.
     *
     * @return A list of aborted transactions.
     */
    public static List<Transaction> getAbortedTransactions() {
        return abortedTransactions;
    }

    /**
     * Runs a transaction by creating a new TransactionManagerWorker thread and
     * starting it.
     *
     * @param client The client socket that initiated the transaction.
     */
    public void runTransaction(Socket client) {
        (new TransactionManagerWorker(client)).start();
    }

}
