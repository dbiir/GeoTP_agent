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

package org.dbiir.harp.backend.handler.admin.executor;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.kernel.authority.checker.AuthorityChecker;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataMergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataQueryResultRow;
import org.dbiir.harp.utils.common.statement.mysql.dal.MySQLShowDatabasesStatement;
import org.dbiir.harp.utils.common.util.SQLUtils;

import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Show databases executor.
 */
@RequiredArgsConstructor
@Getter
public final class ShowDatabasesExecutor implements DatabaseAdminQueryExecutor {
    
    private final MySQLShowDatabasesStatement showDatabasesStatement;
    
    private MergedResult mergedResult;
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        mergedResult = new LocalDataMergedResult(getDatabaseNames(connectionSession));
    }
    
    private Collection<LocalDataQueryResultRow> getDatabaseNames(final ConnectionSession connectionSession) {
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        AuthorityChecker authorityChecker = new AuthorityChecker(authorityRule, connectionSession.getGrantee());
        return ProxyContext.getInstance().getAllDatabaseNames().stream().sorted()
                .filter(each -> checkLikePattern(each) && authorityChecker.isAuthorized(each)).map(LocalDataQueryResultRow::new).collect(Collectors.toList());
    }
    
    private boolean checkLikePattern(final String databaseName) {
        if (showDatabasesStatement.getFilter().isPresent()) {
            Optional<String> pattern = showDatabasesStatement.getFilter().get().getLike().map(optional -> SQLUtils.convertLikePatternToRegex(optional.getPattern()));
            return !pattern.isPresent() || databaseName.matches(pattern.get());
        }
        return true;
    }
    
    @Override
    public QueryResultMetaData getQueryResultMetaData() {
        return new RawQueryResultMetaData(Collections.singletonList(new RawQueryResultColumnMetaData("SCHEMATA", "Database", "schema_name", Types.VARCHAR, "VARCHAR", 255, 0)));
    }
}
