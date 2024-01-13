package org.dbiir.harp.utils.transcation;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import static org.dbiir.harp.utils.transcation.XATransactionState.*;


@Getter
@Setter
@Slf4j
@RequiredArgsConstructor
public class PostgreSQLXAStateMachine implements XAStateMachine {
    private final CustomXID xid;

    @Override
    public XATransactionState NextState(XATransactionState state, boolean isLastQuery, boolean isOnePhase, boolean isPrepareOk) {
        XATransactionState nextState = NUM_OF_STATES;
        switch (state) {
            case ACTIVE -> {
                if (isLastQuery) {
                    nextState = IDLE;
                } else {
                    nextState = ACTIVE;
                }
            }
            case IDLE -> {
                if (isOnePhase) {
                    nextState = COMMITTED;
                } else {
                    nextState = PREPARED;
                }
            }
            case PREPARED -> {
                if (isPrepareOk) {
                    nextState = COMMITTED;
                } else {
                    nextState = FAILED;
                }
            }
            case COMMITTED -> {
                log.warn("XA Transaction {} occur exception, transaction is already commit.", xid.toString());
                nextState = COMMITTED;
            }
            case FAILED -> {
                nextState = ABORTED;
            }
            case ABORTED -> {
                log.warn("XA Transaction {} occur exception, transaction is already aborted.", xid.toString());
                nextState = ABORTED;
            }
            default -> {
                log.error("XA Transaction {} occur exception, state is {}.", xid.toString(), state);
            }
        }

        return nextState;
    }

    @Override
    public XATransactionState NextTwoPhaseState(XATransactionState state, boolean isPrepareOk) {
        return NextState(state, true, false, isPrepareOk);
    }

    /**
     * Generate next control xa command with xid
     *
     * @param state next state generate by function NextState
     * @param isOnePhase commit one phase if only one participant
     * @return next xa command
     */
    @Override
    public String NextControlSQL(XATransactionState state, boolean isOnePhase) {
        String result = "";
        switch (state) {
            case IDLE -> result = "";
            case PREPARED -> result = "prepare transaction " + xid.toString();
            case COMMITTED -> {
                log.error("XA Transaction {} can not commit async.", xid.toString());
                if (isOnePhase) {
                    result = "commit";
                } else {
                    result = "commit prepared " + xid.toString();
                }
            }
            case ABORTED -> result = "rollback prepared " + xid.toString();
            case ACTIVE, FAILED -> {}
            default -> log.error("XA Transaction {} occur exception, state is {}.", xid.toString(), state);
        }

        return result;
    }

    /**
     * Active Rollback command
     *
     * @param state current state
     */
    public XATransactionState ActiveRollback(XATransactionState state) {
        XATransactionState nextState = NUM_OF_STATES;
        switch (state) {
            case ACTIVE -> nextState = IDLE;
            case IDLE, PREPARED, FAILED -> nextState = ABORTED;
            case COMMITTED -> log.error("XA Transaction {} occur exception, transaction is already committed, but wants abort.", xid.toString());
            case ABORTED -> {
                log.warn("XA Transaction {} occur exception, transaction is already aborted.", xid.toString());
                nextState = ABORTED;
            }
            default -> log.error("XA Transaction {} occur exception, state is {}.", xid.toString(), state);
        }

        return nextState;
    }
}
