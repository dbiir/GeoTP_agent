/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbiir.harp.backend.handler.data;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.binder.statement.SQLStatementContext;
import org.dbiir.harp.backend.connector.DatabaseConnectorFactory;
import org.dbiir.harp.backend.handler.data.impl.UnicastDatabaseBackendHandler;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dal.DALStatement;
import org.dbiir.harp.utils.common.statement.dal.SetStatement;
import org.dbiir.harp.utils.common.statement.dml.DoStatement;
import org.dbiir.harp.utils.common.statement.dml.SelectStatement;

import java.util.List;
import java.util.LinkedList;

/**
 * Database backend handler factory.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DatabaseBackendHandlerFactory {
    
    /**
     * New instance of database backend handler.
     *
     * @param queryContext query context
     * @param connectionSession connection session
     * @param preferPreparedStatement use prepared statement as possible
     * @return created instance
     */
    public static DatabaseBackendHandler newInstance(final QueryContext queryContext, final ConnectionSession connectionSession, final boolean preferPreparedStatement) {
        SQLStatementContext<?> sqlStatementContext = queryContext.getSqlStatementContext();
        SQLStatement sqlStatement = sqlStatementContext.getSqlStatement();
        if (sqlStatement instanceof DoStatement) {
            return new UnicastDatabaseBackendHandler(queryContext, connectionSession);
        }
        if (sqlStatement instanceof SetStatement && null == connectionSession.getDatabaseName()) {
            List<ResponseHeader> result = new LinkedList<ResponseHeader>();
            result.add(new UpdateResponseHeader(sqlStatement));
            return () -> result;
        }
        if (sqlStatement instanceof DALStatement || sqlStatement instanceof SelectStatement && null == ((SelectStatement) sqlStatement).getFrom()) {
            return new UnicastDatabaseBackendHandler(queryContext, connectionSession);
        }
        return DatabaseConnectorFactory.getInstance().newInstance(queryContext, connectionSession.getBackendConnection(), preferPreparedStatement);
    }
}
