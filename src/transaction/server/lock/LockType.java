package transaction.server.lock;

/**
 * Enum [LockType] represents different types of locks that can be used by the LockManager.
 *
 * @author manoj
 */
public enum LockType {

    // Indicates that no lock is currently held on the resource.
    EMPTY_LOCK,

    // Indicates that read lock is currently held on the resource.
    READ_LOCK,

    // Indicates that write lock is currently held on the resource.
    WRITE_LOCK;

    /**
     * Promotes the current lock type to the next higher lock type in the hierarchy.
     * If the current lock type is EMPTY_LOCK, it is promoted to READ_LOCK
     * If the current lock type is READ_LOCK, it is promoted to WRITE_LOCK
     *
     * @return the next higher lock type in the hierarchy
     */
    public LockType promote() {
        if (this == EMPTY_LOCK) {
            return READ_LOCK;
        } else if(this == READ_LOCK) {
            return WRITE_LOCK;
        }
        return this;
    }
}