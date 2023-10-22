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
import org.dbiir.harp.backend.context.ProxyContext;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultColumnMetaData;
import org.dbiir.harp.executor.sql.execute.result.query.impl.raw.metadata.RawQueryResultMetaData;
import org.dbiir.harp.kernel.authority.AuthorityRule;
import org.dbiir.harp.merger.result.MergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataMergedResult;
import org.dbiir.harp.merger.result.impl.local.LocalDataQueryResultRow;
import org.dbiir.harp.utils.common.metadata.user.Grantee;
import org.dbiir.harp.utils.common.metadata.user.AgentUser;

import java.sql.Types;
import java.util.Collections;
import java.util.Optional;

/**
 * Show current user executor.
 */
@Getter
public final class ShowCurrentUserExecutor implements DatabaseAdminQueryExecutor {
    
    public static final String FUNCTION_NAME = "current_user()";
    
    public static final String FUNCTION_NAME_ALIAS = "current_user";
    
    private MergedResult mergedResult;
    
    @Override
    public void execute(final ConnectionSession connectionSession) {
        AuthorityRule authorityRule = ProxyContext.getInstance().getContextManager().getMetaDataContexts().getMetaData().getGlobalRuleMetaData().getSingleRule(AuthorityRule.class);
        Optional<Grantee> grantee = authorityRule.findUser(connectionSession.getGrantee()).map(AgentUser::getGrantee);
        mergedResult = new LocalDataMergedResult(Collections.singleton(new LocalDataQueryResultRow(grantee.isPresent() ? grantee.get().toString() : "")));
    }
    
    @Override
    public QueryResultMetaData getQueryResultMetaData() {
        return new RawQueryResultMetaData(Collections.singletonList(new RawQueryResultColumnMetaData("", FUNCTION_NAME, FUNCTION_NAME, Types.VARCHAR, "VARCHAR", 100, 0)));
    }
}
