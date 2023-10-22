package org.dbiir.harp.frontend.async;


import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.exception.BackendConnectionException;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.backend.handler.ProxyBackendHandlerFactory;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.backend.session.transaction.TransactionStatus;
import org.dbiir.harp.db.protocol.event.WriteCompleteEvent;
import org.dbiir.harp.db.protocol.packet.DatabasePacket;
import org.dbiir.harp.frontend.exception.ExpectedExceptions;
import org.dbiir.harp.mode.metadata.MetaDataContexts;
import org.dbiir.harp.parser.rule.SQLParserRule;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.transcation.*;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

@Getter
@Slf4j
public class AgentAsyncPrepare implements Runnable {
    private final ConnectionSession connectionSession;
    private final DatabaseType databaseType;
    private ChannelHandlerContext context;
    private final String sql;
    private final boolean onePhase;

    public AgentAsyncPrepare(ConnectionSession connectionSession, DatabaseType databaseType, String sql, boolean onePhase) {
        this.connectionSession = connectionSession;
        this.databaseType = databaseType;
        this.sql = sql;
        this.onePhase = onePhase;
    }

    public AgentAsyncPrepare(ConnectionSession connectionSession, DatabaseType databaseType, boolean onePhase) {
        this(connectionSession, databaseType, "", onePhase);
    }

    public void setContext(ChannelHandlerContext ctx) {
        this.context = ctx;
    }

    @Override
    public void run() {
        resetConnectionSession();
        CustomXID customXID = connectionSession.getXID();
        assert (AgentAsyncXAManager.getInstance().getXAStates().containsKey(customXID));
        XATransactionState state = AgentAsyncXAManager.getInstance().getXAStates().get(customXID);
        XAStateMachine machine = new MySQLXAStateMachine(customXID);

        String nextCommand = machine.NextControlSQL(state, false);
        XATransactionState nextState = XATransactionState.NUM_OF_STATES;
        try {
            // TODO: 1. execute("xa end");
            executeXACommand(nextCommand);
            nextState = machine.NextTwoPhaseState(state, false);
            AgentAsyncXAManager.getInstance().getXAStates().put(customXID, XATransactionState.IDLE);
            state = nextState;
        } catch (Exception ex) {
            log.debug("async xa end failed. {}", ex.toString());
            AgentAsyncXAManager.getInstance().getXAStates().put(customXID, XATransactionState.ROLLBACK_ONLY);
            AsyncMessageFromAgent message = new AsyncMessageFromAgent(customXID.toString(), XATransactionState.ROLLBACK_ONLY, System.currentTimeMillis(), ex.toString());
            AgentAsyncXAManager.getInstance().modifyMessages(true, message); // add to message queue;
            return;
        } finally {
            resetConnectionSession();
        }

        if (onePhase) {
            return;
        }

        if (!connectionSession.isCurrentTransactionOk()) {
            AsyncMessageFromAgent message = new AsyncMessageFromAgent(customXID.toString(), XATransactionState.IDLE, System.currentTimeMillis(), "");
            AgentAsyncXAManager.getInstance().modifyMessages(true, message); // add to message queue;
            return;
        }

        // xa prepare
        nextCommand = machine.NextControlSQL(state, false);
        try {
            // TODO: 2. execute("xa prepare");
            executeXACommand(nextCommand);
            nextState = machine.NextTwoPhaseState(state, false);
            AgentAsyncXAManager.getInstance().getXAStates().put(customXID, XATransactionState.PREPARED);
            AsyncMessageFromAgent message = new AsyncMessageFromAgent(customXID.toString(), XATransactionState.PREPARED, System.currentTimeMillis(), "");
            AgentAsyncXAManager.getInstance().modifyMessages(true, message); // add to message queue;
        } catch (Exception ex) {
            log.debug("async xa prepare failed. {}", ex.toString());
            AgentAsyncXAManager.getInstance().getXAStates().put(customXID, XATransactionState.FAILED);
            AsyncMessageFromAgent message = new AsyncMessageFromAgent(customXID.toString(), XATransactionState.FAILED, System.currentTimeMillis(), ex.toString());
            AgentAsyncXAManager.getInstance().modifyMessages(true, message); // add to message queue;
        } finally {
            resetConnectionSession();
        }
    }

    private void executeXACommand(String sql) throws SQLException {
        MetaDataContexts metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        SQLParserRule sqlParserRule = metaDataContexts.getMetaData().getGlobalRuleMetaData().getSingleRule(SQLParserRule.class);
        SQLStatement sqlStatement = sqlParserRule.getSQLParserEngine(databaseType.getType()).parse(sql);
        ProxyBackendHandler proxyBackendHandler = ProxyBackendHandlerFactory.newInstance(databaseType, sql, sqlStatement, connectionSession, null);

        proxyBackendHandler.execute();
    }


    private boolean isPrepareFinish(XATransactionState state) {
        return state == XATransactionState.ABORTED || state == XATransactionState.COMMITTED;
    }



    private void resetConnectionSession() {
        connectionSession.clearQueryContext();
        Collection<SQLException> exceptions = Collections.emptyList();
        try {
            connectionSession.getBackendConnection().closeExecutionResources();
        } catch (final BackendConnectionException ex) {
            exceptions = ex.getExceptions().stream().filter(SQLException.class::isInstance).map(SQLException.class::cast).collect(Collectors.toList());
        }

        if (exceptions.size() != 0) {
            SQLException ex = new SQLException("");
            for (SQLException each : exceptions) {
                ex.setNextException(each);
            }
            processException(ex);
        }
    }

    private void processException(final Exception cause) {
        if (!ExpectedExceptions.isExpected(cause.getClass())) {
            log.error("Exception occur: ", cause);
        } else if (log.isDebugEnabled()) {
            log.debug("Exception occur: ", cause);
        }
        context.write(cause);
        context.flush();
    }
}
