package org.dbiir.harp.utils.transcation;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;

@Getter
@RequiredArgsConstructor
public class AsyncMessageFromAgent implements Serializable {
    private final String Xid;

    private final XATransactionState state;

    private final long currentTimeStamp;

    private final String SQLExceptionString;

    @Override
    public String toString() {
        return "message{" +
                "Xid=" + Xid +
                ", state=" + state +
                ", timestamp=" + currentTimeStamp +
                ", exceptionHint=" + SQLExceptionString + "}";
    }
}
