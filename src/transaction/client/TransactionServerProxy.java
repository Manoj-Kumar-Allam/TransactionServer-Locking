package transaction.client;

import transaction.comm.Message;
import transaction.comm.MessageTypes;
import transaction.server.lock.TransactionAbortedException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * The class [TransactionServerProxy] represents a client proxy for the Transaction Server that
 * handles transactions for multiple accounts.
 *
 * @author srinivas
 */
public class TransactionServerProxy implements MessageTypes {

    // network related fields
    private final String host;
    private final int port;
    private Socket dbConnection;
    private ObjectOutputStream writeToNet;
    private ObjectInputStream readFromNet;
    private Integer transactionID = 0;

    /**
     * Constructs a new TransactionServerProxy object with the given host and port.
     *
     * @param host the host name of the transaction server
     * @param port the port number of the transaction server
     */
    public TransactionServerProxy(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Opens a new transaction and returns its ID.
     *
     * @return the ID of the opened transaction
     */
    public int openTransaction() {
        try {
            dbConnection = new Socket(host, port);
            writeToNet = new ObjectOutputStream(dbConnection.getOutputStream());
            readFromNet = new ObjectInputStream(dbConnection.getInputStream());
        } catch (IOException ex) {
            System.err.println("[TransactionServerProxy.openTransaction] Error occurred when opening object streams");
            ex.printStackTrace();
        }

        try {
            writeToNet.writeObject(new Message(OPEN_TRANSACTION, null));
            transactionID = (Integer) readFromNet.readObject();
        } catch (IOException | ClassNotFoundException | NullPointerException ex) {
            System.err.println("[TransactionServerProxy.openTransaction] Error occurred when writing/reading messages");
            ex.printStackTrace();
        }
        return transactionID;
    }

    /**
     * Requests that the current transaction be closed.
     *
     * @return the status of the transaction, either TRANSACTION_COMMITTED or TRANSACTION_ABORTED
     */
    public int closeTransaction() {
        int returnStatus = TRANSACTION_COMMITTED;

        try {
            writeToNet.writeObject(new Message(CLOSE_TRANSACTION, null));
            returnStatus = (int) readFromNet.readObject();

            readFromNet.close();
            writeToNet.close();
            dbConnection.close();
        } catch (Exception ex) {
            System.err.println("[TransactionServerProxy.closeTransaction] Error occurred");
            ex.printStackTrace();
        }
        return returnStatus;
    }

    /**
     * Reads the balance of the account with the specified account number.
     *
     * @param accountNumber the account number to read from
     * @return the balance of the account
     *
     * @throws TransactionAbortedException if the transaction was aborted
     */
    public int read(int accountNumber) throws TransactionAbortedException {
        Message message = new Message(READ_REQUEST, accountNumber);

        try {
            writeToNet.writeObject(message);
            message = (Message) readFromNet.readObject();
        } catch (Exception ex) {
            System.err.println("[TransactionServerProxy.read] Error occurred");
            ex.printStackTrace();
        }

        if(message.getType() == READ_REQUEST_RESPONSE) {
            return (int) message.getContent();
        } else {
            throw new TransactionAbortedException();
        }
    }

    /**
     * Writes the specified amount to the account with the given account number.
     *
     * @param accountNumber the account number to write to
     * @param amount the amount to write
     * @throws TransactionAbortedException if the transaction was aborted
     */
    public void write(int accountNumber, int amount) throws TransactionAbortedException {
        Object[] content = new Object[]{accountNumber, amount};

        Message message = new Message(WRITE_REQUEST, content);
        try {
            writeToNet.writeObject(message);
            message = (Message) readFromNet.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            System.out.println("[TransactionServerProxy.write] Error occurred: IOException | ClassNotFoundException");
            ex.printStackTrace();
            System.err.println("\n\n");
        }

        if(message.getType() == TRANSACTION_ABORTED) {
            // transaction is aborted
            throw new TransactionAbortedException();
        }
    }
}