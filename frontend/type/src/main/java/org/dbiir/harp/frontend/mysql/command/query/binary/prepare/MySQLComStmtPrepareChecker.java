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

package org.dbiir.harp.frontend.mysql.command.query.binary.prepare;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.dal.*;
import org.dbiir.harp.utils.common.statement.mysql.dml.*;
import org.dbiir.harp.utils.common.statement.mysql.rl.MySQLChangeMasterStatement;
import org.dbiir.harp.utils.common.statement.mysql.rl.MySQLStartSlaveStatement;
import org.dbiir.harp.utils.common.statement.mysql.rl.MySQLStopSlaveStatement;
import org.dbiir.harp.utils.common.statement.mysql.tcl.MySQLCommitStatement;
import org.dbiir.harp.utils.common.statement.mysql.tcl.MySQLXAStatement;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

/**
 * COM_STMT_PREPARE command statement checker for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/sql-prepared-statements.html">SQL Syntax Allowed in Prepared Statements</a>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MySQLComStmtPrepareChecker {
    
    private static final Collection<Class<?>> SQL_STATEMENTS_ALLOWED = new HashSet<>();
    
    static {
//        SQL_STATEMENTS_ALLOWED.addAll(Arrays.asList(MySQLAlterTableStatement.class, MySQLAlterUserStatement.class, MySQLAnalyzeTableStatement.class,
//                MySQLCacheIndexStatement.class, MySQLCallStatement.class, MySQLChangeMasterStatement.class, MySQLChecksumTableStatement.class, MySQLCommitStatement.class,
//                MySQLCreateIndexStatement.class, MySQLDropIndexStatement.class, MySQLCreateDatabaseStatement.class, MySQLDropDatabaseStatement.class,
//                MySQLCreateTableStatement.class, MySQLDropTableStatement.class, MySQLCreateUserStatement.class, MySQLRenameUserStatement.class, MySQLDropUserStatement.class,
//                MySQLCreateViewStatement.class, MySQLDropViewStatement.class, MySQLDeleteStatement.class, MySQLDoStatement.class, MySQLFlushStatement.class,
//                MySQLGrantStatement.class, MySQLInsertStatement.class, MySQLInstallPluginStatement.class, MySQLKillStatement.class, MySQLLoadIndexInfoStatement.class,
//                MySQLOptimizeTableStatement.class, MySQLRenameTableStatement.class, MySQLRepairTableStatement.class, MySQLResetStatement.class,
//                MySQLRevokeStatement.class, MySQLSelectStatement.class, MySQLSetStatement.class, MySQLShowWarningsStatement.class, MySQLShowErrorsStatement.class,
//                MySQLShowBinlogEventsStatement.class, MySQLShowCreateProcedureStatement.class, MySQLShowCreateFunctionStatement.class, MySQLShowCreateEventStatement.class,
//                MySQLShowCreateTableStatement.class, MySQLShowCreateViewStatement.class, MySQLShowBinaryLogsStatement.class, MySQLShowStatusStatement.class,
//                MySQLStartSlaveStatement.class, MySQLStopSlaveStatement.class, MySQLTruncateStatement.class, MySQLUninstallPluginStatement.class, MySQLUpdateStatement.class, MySQLXAStatement.class));

        SQL_STATEMENTS_ALLOWED.addAll(Arrays.asList(MySQLAnalyzeTableStatement.class, MySQLCallStatement.class, MySQLChangeMasterStatement.class, MySQLChecksumTableStatement.class,
                MySQLCommitStatement.class, MySQLDeleteStatement.class, MySQLDoStatement.class, MySQLFlushStatement.class,
                MySQLInsertStatement.class, MySQLInstallPluginStatement.class, MySQLKillStatement.class, MySQLOptimizeTableStatement.class, MySQLRepairTableStatement.class,
                MySQLResetStatement.class, MySQLSelectStatement.class, MySQLSetStatement.class, MySQLShowWarningsStatement.class, MySQLShowErrorsStatement.class,
                MySQLShowBinlogEventsStatement.class, MySQLShowCreateProcedureStatement.class, MySQLShowCreateFunctionStatement.class, MySQLShowCreateEventStatement.class,
                MySQLShowCreateTableStatement.class, MySQLShowCreateViewStatement.class, MySQLShowBinaryLogsStatement.class, MySQLShowStatusStatement.class,
                MySQLStartSlaveStatement.class, MySQLStopSlaveStatement.class, MySQLUninstallPluginStatement.class, MySQLUpdateStatement.class, MySQLXAStatement.class));
    }
    
    /**
     * Judge if SQL statement is allowed.
     *
     * @param sqlStatement SQL statement
     * @return SQL statement is allowed or not
     */
    public static boolean isStatementAllowed(final SQLStatement sqlStatement) {
        return SQL_STATEMENTS_ALLOWED.contains(sqlStatement.getClass());
    }
}
