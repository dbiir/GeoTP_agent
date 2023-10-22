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
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentSchema;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentTable;
import org.dbiir.harp.utils.common.segment.dml.assignment.InsertValuesSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.ExpressionSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.simple.ParameterMarkerExpressionSegment;
import org.dbiir.harp.utils.common.segment.generic.ParameterMarkerSegment;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.dml.InsertStatement;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Parameter marker extractor for MySQL COM_STMT_PREPARE.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MySQLComStmtPrepareParameterMarkerExtractor {
    
    /**
     * TODO Support more statements and syntax.
     * Find corresponding columns of parameter markers.
     *
     * @param sqlStatement SQL statement
     * @param schema schema
     * @return map parameter marker segment to column
     */
    public static Map<ParameterMarkerSegment, AgentColumn> findColumnsOfParameterMarkers(final SQLStatement sqlStatement, final AgentSchema schema) {
        return sqlStatement instanceof InsertStatement ? findColumnsOfParameterMarkersForInsert((InsertStatement) sqlStatement, schema) : Collections.emptyMap();
    }
    
    private static Map<ParameterMarkerSegment, AgentColumn> findColumnsOfParameterMarkersForInsert(final InsertStatement insertStatement, final AgentSchema schema) {
        AgentTable table = schema.getTable(insertStatement.getTable().getTableName().getIdentifier().getValue());
        List<String> columnNamesOfInsert = getColumnNamesOfInsertStatement(insertStatement, table);
        Map<String, AgentColumn> columnsOfTable = table.getColumns();
        Map<String, AgentColumn> caseInsensitiveColumnsOfTable = convertToCaseInsensitiveColumnsOfTable(columnsOfTable);
        Map<ParameterMarkerSegment, AgentColumn> result = new LinkedHashMap<>(insertStatement.getParameterMarkerSegments().size(), 1);
        for (InsertValuesSegment each : insertStatement.getValues()) {
            ListIterator<ExpressionSegment> listIterator = each.getValues().listIterator();
            for (int columnIndex = listIterator.nextIndex(); listIterator.hasNext(); columnIndex = listIterator.nextIndex()) {
                ExpressionSegment value = listIterator.next();
                if (!(value instanceof ParameterMarkerExpressionSegment)) {
                    continue;
                }
                String columnName = columnNamesOfInsert.get(columnIndex);
                AgentColumn column = columnsOfTable.getOrDefault(columnName, caseInsensitiveColumnsOfTable.get(columnName));
                if (null != column) {
                    result.put((ParameterMarkerSegment) value, column);
                }
            }
        }
        return result;
    }
    
    private static List<String> getColumnNamesOfInsertStatement(final InsertStatement insertStatement, final AgentTable table) {
        return insertStatement.getColumns().isEmpty() ? new ArrayList<>(table.getColumns().keySet())
                : insertStatement.getColumns().stream().map(each -> each.getIdentifier().getValue()).collect(Collectors.toList());
    }
    
    private static Map<String, AgentColumn> convertToCaseInsensitiveColumnsOfTable(final Map<String, AgentColumn> columns) {
        Map<String, AgentColumn> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        result.putAll(columns);
        return result;
    }
}
