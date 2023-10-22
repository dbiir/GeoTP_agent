package org.dbiir.harp.utils.transcation;

public interface XAStateMachine {
    public XATransactionState NextState(XATransactionState state, boolean isLastQuery, boolean isOnePhase, boolean isPrepareOk);

    public XATransactionState NextTwoPhaseState(XATransactionState state, boolean isPrepareOk);

    public String NextControlSQL(XATransactionState state, boolean isOnePhase);
}
