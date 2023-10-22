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

package org.dbiir.harp.backend.handler.data.impl;

import lombok.RequiredArgsConstructor;

import org.dbiir.harp.backend.connector.DatabaseConnectorFactory;
import org.dbiir.harp.backend.connector.DatabaseConnector;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.handler.data.DatabaseBackendHandler;
import org.dbiir.harp.backend.response.data.QueryResponseRow;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.kernel.authority.model.AgentPrivileges;
import org.dbiir.harp.utils.binder.QueryContext;
import org.dbiir.harp.utils.exceptions.external.NoDatabaseSelectedException;

import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Database backend handler with unicast schema.
 */
@RequiredArgsConstructor
public final class UnicastDatabaseBackendHandler implements DatabaseBackendHandler {
    
    private final DatabaseConnectorFactory databaseConnectorFactory = DatabaseConnectorFactory.getInstance();
    
    private final QueryContext queryContext;
    
    private final ConnectionSession connectionSession;
    
    private DatabaseConnector databaseConnector;
    
    @Override
    public List<ResponseHeader> execute() throws SQLException {
        List<ResponseHeader> result = new LinkedList<>();
        String originDatabase = connectionSession.getDefaultDatabaseName();
        String databaseName = null == originDatabase ? getFirstDatabaseName() : originDatabase;
        try {
            connectionSession.setCurrentDatabase(databaseName);
            databaseConnector = databaseConnectorFactory.newInstance(queryContext, connectionSession.getBackendConnection(), false);
            return databaseConnector.execute();
        } finally {
            connectionSession.setCurrentDatabase(databaseName);
        }
    }
    
    private String getFirstDatabaseName() {
        Collection<String> databaseNames = ProxyContext.getInstance().getAllDatabaseNames();
        if (databaseNames.isEmpty()) {
            throw new NoDatabaseSelectedException();
        }
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        Optional<AgentPrivileges> privileges = authorityRule.findPrivileges(connectionSession.getGrantee());
        Stream<String> databaseStream = databaseNames.stream().filter(each -> ProxyContext.getInstance().getDatabase(each).containsDataSource());
        Optional<String> result = privileges.map(optional -> databaseStream.filter(optional::hasPrivileges).findFirst()).orElseGet(databaseStream::findFirst);
        return result.get();
    }
    
    @Override
    public boolean next() throws SQLException {
        return databaseConnector.next();
    }
    
    @Override
    public QueryResponseRow getRowData() throws SQLException {
        return databaseConnector.getRowData();
    }
    
    @Override
    public void close() throws SQLException {
        if (null != databaseConnector) {
            databaseConnector.close();
        }
    }
}
