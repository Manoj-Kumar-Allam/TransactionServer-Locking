package transaction.server.lock;

import transaction.server.account.Account;
import transaction.server.transaction.Transaction;

import java.util.*;

/**
 * A class representing a lock that protects an {@link Account} object.
 * If a deadlock is detected, the transaction will abort and throw a {@link TransactionAbortedException}.
 *
 * @author manoj and sampath
 */
public class Lock {

    // the account this lock protects
    private final Account account;

    // the current lock type
    private LockType currentLockType;

    // a list of transactions holding the lock
    private final List<Transaction> lockHolders;

    // a map of transactions requesting the lock and their requested lock type
    private final Map<Transaction, LockType> lockRequesters;

    // a string used as a prefix for log messages
    private static String prefixLogString = "[Lock.acquire]";


    /**
     * Creates a new {@link Lock} object that protects the specified account.
     *
     * @param account The account to protect with this lock.
     */
    public Lock(Account account) {
        this.account = account;

        this.lockHolders = new ArrayList<>();
        this.lockRequesters = new HashMap<>();

        this.currentLockType = LockType.EMPTY_LOCK;
    }

    /**
     * Acquires this lock in the specified mode for the specified transaction.
     *
     * @param transaction The transaction trying to set the lock.
     * @param newLockType The lock type to be set.
     *
     * @throws TransactionAbortedException If a deadlock is detected while acquiring the lock.
     */
    public synchronized void acquire(Transaction transaction, LockType newLockType) throws TransactionAbortedException {
        transaction.log(prefixLogString + " try to set " + getLockTypeString(newLockType) +
        " on account # " + account.getAccountNumber());

        // check for deadlocks with other transactions holding locks
        while(isConflict(transaction, newLockType)) {

            List<Lock> locks = transaction.getLocks();

            Iterator<Lock> lockIterator = locks.iterator();

            Lock checkedLock;

            while(lockIterator.hasNext()) {
                checkedLock = lockIterator.next();

                // abort the transaction if a deadlock is detected
                if(!checkedLock.lockRequesters.isEmpty()) {
                    transaction.log(prefixLogString +
                            " aborting when trying to set a " + getLockTypeString(newLockType) +
                            " on account #" + account.getAccountNumber() +
                            " while holding a " + getLockTypeString(checkedLock.currentLockType) +
                            " on account #" + checkedLock.account.getAccountNumber());
                    throw new TransactionAbortedException();
                }
            }

            transaction.log(prefixLogString +
                    " ---> wait to set " + getLockTypeString(newLockType) +
                    " on account #" + account.getAccountNumber());

            addLockRequester(transaction, newLockType);

            try {
                System.out.println("[LockManager.acquire] Transaction with id #" + transaction.getTransactionId() +
                        " went into wait state, while acquiring lock on Account #" + account.getAccountNumber());
                wait();
                System.out.println("[LockManager.acquire] Transaction with id #" + transaction.getTransactionId() +
                        " is released for Account #" + account.getAccountNumber());
            } catch (InterruptedException ex) {
                // ignore
            }

            removeLockRequester(transaction);


            transaction.log(prefixLogString +
                    " <--- woke up again trying to set " + getLockTypeString(newLockType) +
                    " on account #" + account.getAccountNumber());
        }

        // At this point, we are not in conflict, set the lock
        if (lockHolders.isEmpty()) {
            lockHolders.add(transaction);
            currentLockType = newLockType;
        } else if (lockHolders.stream().anyMatch(lockedTransaction -> lockedTransaction != transaction)) {
            lockHolders.add(transaction);
        } else {
            // promote the lock
            currentLockType = currentLockType.promote();
        }
    }

    private void removeLockRequester(Transaction transaction) {
        lockRequesters.remove(transaction);
    }

    private void addLockRequester(Transaction transaction, LockType newLockType) {
        lockRequesters.put(transaction, newLockType);
    }

    /**
     * Determines whether a lock request from the given transaction with the given lock type
     * conflicts with the current locks held by other transactions.
     *
     * @param transaction The transaction requesting the lock.
     * @param newLockType The lock type requested by the transaction.
     *
     * @return true if the lock request conflicts with existing locks, false otherwise
     */
    private boolean isConflict(Transaction transaction, LockType newLockType) {
        if (lockHolders.isEmpty()) {
            return false;
        }

        if (lockHolders.size() == 1 && lockHolders.get(0) == transaction) {
            return false;
        }

        if (newLockType == LockType.READ_LOCK) {
            boolean writeLockFound = false;
            for (Transaction lockHolder : lockHolders) {
                if (lockHolder == transaction) {
                    continue;
                }
                if (lockHolder.getLocks().stream()
                        .anyMatch(lock -> lock.currentLockType == LockType.WRITE_LOCK)) {
                    writeLockFound = true;
                    break;
                }
            }
            return writeLockFound;
        }

        return true;
    }


    /**
     * Gets the lock type in a string format based on the given {@link LockType}
     * @param lockType the lock type
     *
     * @return the lock type string
     */
    private String getLockTypeString(LockType lockType) {
        switch (lockType) {
            case READ_LOCK:
                return "READ_LOCK";
            case WRITE_LOCK:
                return "WRITE_LOCK";
            default:
                return "EMPTY_LOCK";
        }
    }

    /**
     * Removes the given transaction from the list of lock holders and sets the current lock type to empty.
     * notifies the waiting threads
     *
     * @param transaction the transaction that wants to release the lock
     */
    public synchronized void release(Transaction transaction) {
        if (lockHolders.contains(transaction)) {
            System.out.println("[LockManager.release] Transaction with id #" + transaction.getTransactionId() +
                    " is released all its locks");
            lockHolders.remove(transaction);
            notifyAll();
        }
    }
}
