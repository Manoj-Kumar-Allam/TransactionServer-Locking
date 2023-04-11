package transaction.server.lock;

import transaction.server.account.Account;
import transaction.server.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * class [LockManager] manages the locks for accounts
 *
 * @author manoj and sampath
 */
public class LockManager {

    // locks keep track of the current locks held on accounts
    private Map<Account, Lock> locks;

    /**
     * Constructor which constructs a new LockManager with an empty lock map.
     */
    public LockManager() {
        this.locks = new HashMap<>();
    }

    /**
     * Acquires a lock of the specified type on the given account for the specified transaction.
     *
     * @param account the account to acquire the lock on
     * @param transaction the transaction that wants to acquire the lock
     * @param lockType the type of lock to acquire
     * @throws TransactionAbortedException if the transaction is aborted while trying to acquire the lock
     */
    public void setLock(Account account, Transaction transaction, LockType lockType) throws TransactionAbortedException {
        Lock lock = getOrCreateLock(account);
        lock.acquire(transaction, lockType);
        if(!transaction.getLocks().contains(lock)) {
            transaction.addLock(lock);
        }
    }

    /**
     * Releases all locks held by the specified transaction.
     *
     * @param transaction the transaction to release locks for
     */
    public synchronized void unlock(Transaction transaction) {
        transaction.getLocks().forEach(lock -> lock.release(transaction));
        transaction.getLocks().clear();
    }

    /**
     * Returns the lock for the specified account, creating it if necessary.
     *
     * @param account the account to get the lock for
     * @return the lock for the account
     */
    private synchronized Lock getOrCreateLock(Account account) {
        Lock lock = locks.get(account);
        if (lock == null) {
            lock = new Lock(account);
            locks.put(account, lock);
        }
        return lock;
    }
}