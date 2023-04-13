package transaction.client;

import transaction.comm.Message;
import transaction.comm.MessageTypes;
import transaction.server.lock.TransactionAbortedException;
import utils.PropertyHandler;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * The TransactionClient class represents a client that creates multiple threads for executing transactions between
 * random pairs of accounts.
 *
 * @author manaoj and srinivas
 */
public class TransactionClient implements Runnable {

    private int numberOfAccounts;
    private int initialBalance;
    private final String serverIP;
    private int serverPort;
    private int numberOfTransactions;
    private Properties properties;

    /**
     * Constructs a new TransactionClient object with the specified properties file.
     * @param propertiesFile the name of the properties file to read from
     */
    public TransactionClient(String propertiesFile) {
        try {
            properties = new PropertyHandler(propertiesFile);
        } catch (IOException e) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read properties file");
            e.printStackTrace();
            System.exit(1);
        }
        serverIP = properties.getProperty("SERVER_IP");

        if(serverIP == null) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read SERVER_IP property");
            System.exit(1);
        }

        try {
            serverPort = Integer.parseInt(properties.getProperty("SERVER_PORT"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Server Port");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            numberOfAccounts = Integer.parseInt(properties.getProperty("NUMBER_OF_ACCOUNTS"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Number Of Accounts");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            initialBalance = Integer.parseInt(properties.getProperty("INITIAL_BALANCE"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Initial Balance");
            ex.printStackTrace();
            System.exit(1);
        }

        try {
            numberOfTransactions = Integer.parseInt(properties.getProperty("NUMBER_OF_TRANSACTIONS"));
        } catch (NumberFormatException ex) {
            System.out.println("[TransactionClient.TransactionClient] couldn't read Number of Transactions");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    /*
     * This method creates transaction executors and runs them in separate threads.
     */
    @Override
    public void run() {
        // Create the transaction server proxy
        TransactionServerProxy transactionServerProxy;

        List<Thread> transactionThreadList = new ArrayList<>();

        // Create the transaction executors and run them in separate threads
        for (int i = 1; i <= numberOfTransactions; i++) {

            int accountA = 0;
            int accountB = 0;

            while((accountA == 0 || accountB == 0) || accountA == accountB) {
                accountA = (int) (Math.random() * numberOfAccounts) + 1;
                accountB = (int) (Math.random() * numberOfAccounts) + 1;
//                accountA = 2;
//                accountB = 4;
            }

            int amount = 5;

            // Create the transaction server proxy
            transactionServerProxy = new TransactionServerProxy(serverIP, serverPort);

            TransactionThread transaction = new TransactionThread(accountA, accountB, amount, transactionServerProxy);
            Thread transactionThread = new Thread(transaction);
            transactionThread.start();

            transactionThreadList.add(transactionThread);
        }

        Iterator<Thread> iterator = transactionThreadList.iterator();

        while (iterator.hasNext()) {
            Thread thread = iterator.next();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // we don't care
            }
        }

        Socket dbConnection = null;
        ObjectOutputStream writeToNet = null;

        try {
            dbConnection = new Socket(serverIP, serverPort);
            writeToNet = new ObjectOutputStream(dbConnection.getOutputStream());
            writeToNet.writeObject(new Message(MessageTypes.SHUTDOWN, null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                writeToNet.close();
                dbConnection.close();
            } catch (IOException e) {
                // we don't care
            }
        }
    }

    public static void main(String[] args) {
        String propertiesFile = null;

        try {
            propertiesFile = args[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            propertiesFile = "resources/transaction_client.properties";
        }

        new Thread(new TransactionClient(propertiesFile)).start();
    }

    /**
     * This class represents a transaction executor, which runs transactions in separate threads.
     */
    private class TransactionThread implements Runnable {

        private final int accountA;
        private final int accountB;
        private final int amount;
        private final TransactionServerProxy transactionServerProxy;

        /**
         * Constructs a transaction executor with the specified parameters.
         * @param accountA the account A for the transaction
         * @param accountB the account B for the transaction
         * @param amount the amount to be transferred in the transaction
         * @param transactionServerProxy the transaction server proxy to use for the transaction
         */
        public TransactionThread(int accountA, int accountB, int amount, TransactionServerProxy transactionServerProxy) {
            this.accountA = accountA;
            this.accountB = accountB;
            this.amount = amount;
            this.transactionServerProxy = transactionServerProxy;
        }

        /**
         * Runs the transaction executor.
         * This method is called when the thread is started.
         */
        @Override
        public void run() {
            while (true) {
                // Start a new transaction
                transactionServerProxy.openTransaction();

                try {
                    // Read the balance of account A
                    int accountABalance = transactionServerProxy.read(accountA);
                    int deductedBalance = accountABalance - amount;

                    // Write the new balance of account A
                    transactionServerProxy.write(accountA, deductedBalance);

                    // Read the balance of account B
                    int accountBBalance = transactionServerProxy.read(accountB);

                    // Write the new balance of account B
                    transactionServerProxy.write(accountB, accountBBalance + amount);

                    // Close the transaction
                    int status = transactionServerProxy.closeTransaction();
                    if (status == TransactionServerProxy.TRANSACTION_COMMITTED) {
                        System.out.println("Transaction committed successfully.");
                        break;
                    } else {
                        System.out.println("Transaction aborted. Retrying...");
                    }
                } catch (TransactionAbortedException ex) {
                    // If the transaction is aborted, retry
                    System.out.println("Transaction aborted. Retrying...");
                }
            }
        }
    }
}
