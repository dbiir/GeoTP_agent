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

package org.dbiir.harp.backend.response.header.query;

import org.dbiir.harp.executor.sql.execute.result.query.QueryResultMetaData;
import org.dbiir.harp.utils.common.metadata.database.AgentDatabase;
import org.dbiir.harp.utils.common.spi.annotation.SingletonSPI;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPI;

import java.sql.SQLException;

/**
 * Query header builder.
 */
@SingletonSPI
public interface QueryHeaderBuilder extends TypedSPI {
    
    /**
     * Build query header.
     * 
     * @param queryResultMetaData query result meta data
     * @param database database
     * @param columnName column name
     * @param columnLabel column label
     * @param columnIndex column index
     * @return query header
     * @throws SQLException SQL exception
     */
    QueryHeader build(QueryResultMetaData queryResultMetaData, AgentDatabase database, String columnName, String columnLabel, int columnIndex) throws SQLException;
}
