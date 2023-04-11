package transaction.server;

import transaction.server.account.AccountManager;
import transaction.server.lock.LockManager;
import transaction.server.transaction.TransactionManager;
import utils.NetworkUtilities;
import utils.PropertyHandler;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;

/**
 * The [TransactionServer] class is responsible for managing the server-side
 * components of the transaction system, including the account manager, transaction
 * manager, and lock manager. It also creates a server socket to listen for incoming
 * client requests.
 *
 * @author manoj
 */
public class TransactionServer implements Runnable {

    // manager objects
    public static AccountManager accountManager;
    public static TransactionManager transactionManager;
    public static LockManager lockManager;

    // the server socket to accept incoming clients' requests
    static ServerSocket serverSocket;

    // flag indicating to keep running the server loop
    static boolean keepGoing = true;

    // unique counter to number log message, so they can be ordered how they occurred
    static int messageCounter = 0;

    // flag for logging purposes. decides to either show logs from a transaction's perspective
    // or reflecting the program execution
    public static boolean transactionView = true;

    public static int numberOfAccounts;

    // fetch serverIP
    String serverIP = NetworkUtilities.getMyIP();

    /**
     * Constructor for the TransactionServer class. Reads properties from a given file
     * and uses them to initialize the account manager, transaction manager, and lock
     * manager. Also creates a server socket for listening to incoming client requests.
     *
     * @param propertiesFile the path to the properties file containing server configuration
     */
    public TransactionServer(String propertiesFile) {
        Properties properties = null;

        try {
            properties = new PropertyHandler(propertiesFile);
        } catch (IOException e) {
            System.out.println("[TransactionServer.TransactionServer] didn't find the properties file");
            System.exit(1);
        }

        // get number of accounts
        numberOfAccounts = Integer.parseInt(properties.getProperty("NUMBER_OF_ACCOUNTS"));

        // get initial balance
        int initialBalance = Integer.parseInt(properties.getProperty("INITIAL_BALANCE"));

        // create account manager
        accountManager = new AccountManager(numberOfAccounts, initialBalance);
        System.out.println("[TransactionServer.TransactionServer] Account Manager created");

        // create transaction manager
        transactionManager = new TransactionManager();

        System.out.println("[TransactionServer.TransactionServer] Transaction Manager created");

        // create lock manager
        lockManager = new LockManager();
        System.out.println("[TransactionServer.TransactionServer] Lock Manager created");

        try {
            // get port
            int port = Integer.parseInt(properties.getProperty("SERVER_PORT"));

            // create server socket
            serverSocket = new ServerSocket(port, 50, InetAddress.getByName(serverIP));
        } catch (IOException e) {
            System.out.println("[TransactionServer.TransactionServer] couldn't create server socket");
            System.exit(1);
        }
    }

    /**
     * Returns the current message counter and increments it by one.
     *
     * @return the current message counter
     */
    public static int getMessageCount() {
        return ++messageCounter;
    }

    /**
     * Shuts down the server by setting the keepGoing flag to false and closing the server socket.
     */
    public static void shutDown() {
        try {
            keepGoing = false;
            serverSocket.close();
            System.exit(1);
        } catch (IOException e) {
            // we don't care
        }
    }

    /**
     * The main run loop for the TransactionServer. Accepts incoming client requests
     * and passes them to the transaction manager for processing.
     */
    @Override
    public void run() {
        try {
            while (keepGoing) {
                // run the transaction
                transactionManager.runTransaction(serverSocket.accept());
            }
        } catch (IOException e) {
//            System.out.println("[TransactionServer.run] Error in creating server socket");
        }
    }

    // entry point of the Transaction Server
    public static void main(String[] args) {
        String propertiesFile;

        try {
            propertiesFile = args[0];
        } catch (ArrayIndexOutOfBoundsException ex) {
            propertiesFile = "resources/transaction_server.properties";
        }

        // create Transaction Server
        new Thread(new TransactionServer(propertiesFile)).start();
    }
}
