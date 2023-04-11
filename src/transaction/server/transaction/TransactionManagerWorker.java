package transaction.server.transaction;

import transaction.comm.Message;
import transaction.comm.MessageTypes;
import transaction.server.TransactionServer;
import transaction.server.account.Account;
import transaction.server.lock.TransactionAbortedException;
import utils.TerminalColors;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

import static transaction.server.transaction.TransactionManager.*;

/**
 * A worker thread that handles a client's transaction requests.
 * It reads messages from the client and takes appropriate actions based on the message type.
 *
 * @author manoj, srinivas, sampath
 */
public class TransactionManagerWorker extends Thread implements MessageTypes, TerminalColors {

    // network communication related fields
    Socket client;
    ObjectInputStream readFromNet;
    ObjectOutputStream writeToNet;
    Message message;

    // transaction related properties
    Transaction transaction = null;
    int accountNumber = 0;
    int balance = 0;

    // flag for jumping out of while loop after this transaction closed
    boolean keepGoing = true;

    /**
     * Constructs a new TransactionManagerWorker with the given client socket.
     * Initializes object streams for communication with the client.
     *
     * @param client the client socket to communicate with
     */
    public TransactionManagerWorker(Socket client) {
        this.client = client;

        try {
            // open object streams
            readFromNet =new ObjectInputStream(client.getInputStream());
            writeToNet = new ObjectOutputStream(client.getOutputStream());
        } catch (IOException ex) {
            System.out.println("[TransactionManagerWorker.run] Failed to open object streams");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Thread entry point for the TransactionManagerWorker thread.
     * Handles incoming messages from the client Socket and responds accordingly.
     */
    @Override
    public void run() {

        // loop is left when transaction closes
        while(keepGoing) {
            // reading message
            try {
                message = (Message) readFromNet.readObject();
            } catch (IOException | ClassNotFoundException ex) {
                System.out.println("[TransactionManagerWorker.run] Client shut down, shutting down as well ...");
                TransactionServer.shutDown();
                return;
            }

            // processing message
            switch (message.getType()) {

                // =====================================================================================================
                case OPEN_TRANSACTION:
                // =====================================================================================================
                    synchronized (runningTransactions) {
                        // create new transaction and add it to running transactions
                        transaction = new Transaction(++TransactionManager.transactionIdCounter);
                        runningTransactions.add(transaction);
                    }

                    try {
                        writeToNet.writeObject(transaction.getTransactionId());
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] OPEN_TRANSACTION #"
                                + transaction.getTransactionId() + " - Error writing transactionID to the client");
                    }

                    System.out.println("[TransactionManagerWorker.run] " +  " OPEN_TRANSACTION" + " #" + transaction.getTransactionId());

                    // log transaction event
                    transaction.log("[TransactionManagerWorker.run] " + OPEN_COLOR + "OPEN_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionId());
                    break;

                // =====================================================================================================
                case CLOSE_TRANSACTION:
                // =====================================================================================================

                    // unlock transaction, remove it from running transactions and add it to committed transactions
                    TransactionServer.lockManager.unlock(transaction);
                    runningTransactions.remove(transaction);
                    committedTransactions.add(transaction);

                    try {
                        // just commit, deadlocks were resolved in the read and write requests
                        writeToNet.writeObject(TRANSACTION_COMMITTED);

                        readFromNet.close();
                        writeToNet.close();
                        client.close();
                        // bail out
                        keepGoing = false;
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] CLOSE_TRANSACTION #"
                                + transaction.getTransactionId() + " - Error when closing connection to client");
                    }

                    // log the action
                    transaction.log("[TransactionManagerWorker.run] " + COMMIT_COLOR + "CLOSE_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionId());

                    System.out.println("[TransactionManagerWorker.run] " + " CLOSE_TRANSACTION"
                            + " #" + transaction.getTransactionId());

                    System.out.println(transaction.getLog());
                    keepGoing = false;
                    break;

                // READ_REQUEST case: Reads the balance of the specified account
                // =====================================================================================================
                case READ_REQUEST:
                // =====================================================================================================
                    // read request
                    accountNumber = (int) message.getContent();
                    try {
                        // get the balance by acquiring the Read lock
                        balance = TransactionServer.accountManager.read(accountNumber, transaction);
                        message = new Message(READ_REQUEST_RESPONSE, balance);

                        System.out.println("[TransactionManagerWorker.run] " + " READ_TRANSACTION"
                                + " #" + transaction.getTransactionId() + " for account #" + accountNumber
                                + " with balance " + balance + " - successful");
                    } catch (TransactionAbortedException e) {
                        message = new Message(TRANSACTION_ABORTED, null);

                        synchronized (TransactionServer.accountManager) {
                            // Roll back changes made to the accounts in the Before Image
                            for(Map.Entry<Integer, Integer> entry :  transaction.getBeforeImage().entrySet()) {
                                int accNumber = entry.getKey();
                                int bal = entry.getValue();

                                Account account = TransactionServer.accountManager.getAccountByAccountNumber(accNumber);
                                account.setBalance(bal);
                            }
                        }

                        transaction.log("[TransactionManagerWorker.run] " + ABORT_COLOR + "READ_TRANSACTION" + RESET_COLOR
                                + " #" + transaction.getTransactionId() + " - ABORTED");
                        System.out.println("[TransactionManagerWorker.run] " + " READ_TRANSACTION"
                                + " #" + transaction.getTransactionId() + " - ABORTED");

                        // Unlock the transaction, remove it from the running list, and add it to the aborted list
                        TransactionServer.lockManager.unlock(transaction);

                        runningTransactions.remove(transaction);
                        abortedTransactions.add(transaction);
                        System.out.println(transaction.getLog());
                        keepGoing = false;
                    }

                    try {
                        writeToNet.writeObject(message);
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] READ_TRANSACTION #"
                                + transaction.getTransactionId() + " - Error writing Account Balance to the client");
                    }

                    transaction.log("[TransactionManagerWorker.run] " + READ_COLOR + "READ_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionId() + " - READ");
                    break;

                // WRITE_REQUEST case: Writes a new balance for the specified account
                // =====================================================================================================
                case WRITE_REQUEST:
                // =====================================================================================================
                    Object[] content = (Object[]) message.getContent();
                    // get the account number and balance to update the account
                    accountNumber = (int) content[0];
                    balance = (int) content[1];

                    try {
                        TransactionServer.accountManager.write(accountNumber, balance, transaction);
                        System.out.println("[TransactionManagerWorker.run] " + " WRITE_TRANSACTION"
                                + " #" + transaction.getTransactionId() + " for account #" + accountNumber +
                                " and balance " + balance + " - successful");
                    } catch (TransactionAbortedException e) {

                        message = new Message(TRANSACTION_ABORTED, null);

                        synchronized (TransactionServer.accountManager) {
                            // Roll back changes made to the accounts in the Before Image
                            for(Map.Entry<Integer, Integer> entry :  transaction.getBeforeImage().entrySet()) {
                                int accNumber = entry.getKey();
                                int bal = entry.getValue();

                                Account account = TransactionServer.accountManager.getAccountByAccountNumber(accNumber);
                                account.setBalance(bal);
                            }
                        }

                        System.out.println("[TransactionManagerWorker.run] " + " WRITE_TRANSACTION"
                                + " #" + transaction.getTransactionId() + " - ABORTED");

                        transaction.log("[TransactionManagerWorker.run] " + ABORT_COLOR + "WRITE_TRANSACTION" + RESET_COLOR
                                + " #" + transaction.getTransactionId() + " - ABORTED");

                        // Unlock the transaction, remove it from the running list, and add it to the aborted list
                        TransactionServer.lockManager.unlock(transaction);

                        runningTransactions.remove(transaction);
                        abortedTransactions.add(transaction);
                        System.out.println(transaction.getLog());
                        keepGoing = false;
                    }

                    try {
                        writeToNet.writeObject(message);
                    } catch (IOException e) {
                        System.err.println("[TransactionManagerWorker.run] READ_TRANSACTION #"
                                + transaction.getTransactionId() + " - Error writing WRITE RESPONSE to the client");
                    }

                    transaction.log("[TransactionManagerWorker.run] " + WRITE_COLOR + "WRITE_TRANSACTION" + RESET_COLOR
                            + " #" + transaction.getTransactionId() + " - WRITE");
                    break;

                case SHUTDOWN:
                    int sumOfAllAccounts = 0;

                    for(int i=1; i <= TransactionServer.numberOfAccounts; i++) {
                        int accountBalance = TransactionServer.accountManager._read(i);
                        sumOfAllAccounts += accountBalance;
                        System.out.println("[TransactionManagerWorker.run]" + "After all the Transactions" +
                                " Account #" + i + " balance " + accountBalance);
                    }

                    System.out.println("[TransactionManagerWorker.run]" + " After all the Transactions" +
                            " sum of all accounts balance " + sumOfAllAccounts);

                    TransactionServer.shutDown();

                    break;
            }
        }
    }
}
