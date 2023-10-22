package org.dbiir.harp.utils.transcation;

public enum XATransactionState {
    ACTIVE,

    IDLE,

    PREPARED,

    COMMITTED,

    FAILED,

    ABORTED,

    ROLLBACK_ONLY,

    NUM_OF_STATES
}
