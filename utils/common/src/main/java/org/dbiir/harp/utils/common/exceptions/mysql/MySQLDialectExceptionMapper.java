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

package org.dbiir.harp.utils.common.exceptions.mysql;


import org.dbiir.harp.utils.exceptions.external.*;
import org.dbiir.harp.utils.exceptions.external.connection.TooManyConnectionsException;
import org.dbiir.harp.utils.exceptions.external.data.InsertColumnsAndValuesMismatchedException;
import org.dbiir.harp.utils.common.exceptions.mapper.SQLDialectExceptionMapper;
import org.dbiir.harp.utils.common.exceptions.mysql.vendor.MySQLVendorError;
import org.dbiir.harp.utils.exceptions.external.sql.type.generic.UnknownSQLException;
import org.dbiir.harp.utils.exceptions.external.sql.vendor.VendorError;
import org.dbiir.harp.utils.exceptions.external.syntax.DatabaseCreateExistsException;
import org.dbiir.harp.utils.exceptions.external.syntax.NoSuchTableException;
import org.dbiir.harp.utils.exceptions.external.syntax.TableExistsException;
import org.dbiir.harp.utils.exceptions.external.transaction.TableModifyInTransactionException;

import java.sql.SQLException;

/**
 * MySQL dialect exception mapper.
 */
public final class MySQLDialectExceptionMapper implements SQLDialectExceptionMapper {
    
    @Override
    public SQLException convert(final SQLDialectException sqlDialectException) {
        if (sqlDialectException instanceof UnknownDatabaseException) {
            return null != ((UnknownDatabaseException) sqlDialectException).getDatabaseName()
                    ? toSQLException(MySQLVendorError.ER_BAD_DB_ERROR, ((UnknownDatabaseException) sqlDialectException).getDatabaseName())
                    : toSQLException(MySQLVendorError.ER_NO_DB_ERROR);
        }
        if (sqlDialectException instanceof NoDatabaseSelectedException) {
            return toSQLException(MySQLVendorError.ER_NO_DB_ERROR);
        }
        if (sqlDialectException instanceof DatabaseCreateExistsException) {
            return toSQLException(MySQLVendorError.ER_DB_CREATE_EXISTS_ERROR, ((DatabaseCreateExistsException) sqlDialectException).getDatabaseName());
        }
        if (sqlDialectException instanceof DatabaseDropNotExistsException) {
            return toSQLException(MySQLVendorError.ER_DB_DROP_NOT_EXISTS_ERROR, ((DatabaseDropNotExistsException) sqlDialectException).getDatabaseName());
        }
        if (sqlDialectException instanceof TableExistsException) {
            return toSQLException(MySQLVendorError.ER_TABLE_EXISTS_ERROR, ((TableExistsException) sqlDialectException).getTableName());
        }
        if (sqlDialectException instanceof NoSuchTableException) {
            return toSQLException(MySQLVendorError.ER_NO_SUCH_TABLE, ((NoSuchTableException) sqlDialectException).getTableName());
        }
        if (sqlDialectException instanceof InsertColumnsAndValuesMismatchedException) {
            return toSQLException(MySQLVendorError.ER_WRONG_VALUE_COUNT_ON_ROW, ((InsertColumnsAndValuesMismatchedException) sqlDialectException).getMismatchedRowNumber());
        }
        if (sqlDialectException instanceof TableModifyInTransactionException) {
            return toSQLException(MySQLVendorError.ER_ERROR_ON_MODIFYING_GTID_EXECUTED_TABLE, ((TableModifyInTransactionException) sqlDialectException).getTableName());
        }
        if (sqlDialectException instanceof TooManyConnectionsException) {
            return toSQLException(MySQLVendorError.ER_CON_COUNT_ERROR);
        }
        if (sqlDialectException instanceof UnsupportedPreparedStatementException) {
            return toSQLException(MySQLVendorError.ER_UNSUPPORTED_PS);
        }
        if (sqlDialectException instanceof UnknownCharsetException) {
            return toSQLException(MySQLVendorError.ER_UNKNOWN_CHARACTER_SET, ((UnknownCharsetException) sqlDialectException).getCharset());
        }
        if (sqlDialectException instanceof UnknownCollationException) {
            return toSQLException(MySQLVendorError.ER_UNKNOWN_COLLATION, ((UnknownCollationException) sqlDialectException).getCollationId());
        }
        if (sqlDialectException instanceof HandshakeException) {
            return toSQLException(MySQLVendorError.ER_HANDSHAKE_ERROR);
        }
        if (sqlDialectException instanceof AccessDeniedException) {
            AccessDeniedException ex = (AccessDeniedException) sqlDialectException;
            return toSQLException(MySQLVendorError.ER_ACCESS_DENIED_ERROR, ex.getUsername(), ex.getHostname(), ex.isUsingPassword() ? "YES" : "NO");
        }
        if (sqlDialectException instanceof DatabaseAccessDeniedException) {
            DatabaseAccessDeniedException ex = (DatabaseAccessDeniedException) sqlDialectException;
            return toSQLException(MySQLVendorError.ER_DBACCESS_DENIED_ERROR, ex.getUsername(), ex.getHostname(), ex.getDatabaseName());
        }
        return new UnknownSQLException(sqlDialectException).toSQLException();
    }
    
    private SQLException toSQLException(final VendorError vendorError, final Object... messageArgs) {
        return new SQLException(String.format(vendorError.getReason(), messageArgs), vendorError.getSqlState().getValue(), vendorError.getVendorCode());
    }
    
    @Override
    public String getType() {
        return "MySQL";
    }
}
