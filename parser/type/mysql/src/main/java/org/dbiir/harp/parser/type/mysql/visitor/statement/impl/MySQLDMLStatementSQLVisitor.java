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

package org.dbiir.harp.parser.type.mysql.visitor.statement.impl;

import lombok.NoArgsConstructor;
import org.dbiir.harp.parser.api.visitor.operation.SQLStatementVisitor;
import org.dbiir.harp.parser.api.visitor.type.DMLSQLVisitor;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CallContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DoStatementContext;
import org.dbiir.harp.utils.common.ASTNode;
import org.dbiir.harp.utils.common.segment.dml.expr.ExpressionSegment;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLCallStatement;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLDoStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * DML Statement SQL visitor for MySQL.
 */
@NoArgsConstructor
public final class MySQLDMLStatementSQLVisitor extends MySQLStatementSQLVisitor implements DMLSQLVisitor, SQLStatementVisitor {
    
    public MySQLDMLStatementSQLVisitor(final Properties props) {
        super(props);
    }
    
    @Override
    public ASTNode visitCall(final CallContext ctx) {
        List<ExpressionSegment> params = new ArrayList<>();
        ctx.expr().forEach(each -> params.add((ExpressionSegment) visit(each)));
        return new MySQLCallStatement(ctx.identifier().getText(), params);
    }
    
    @Override
    public ASTNode visitDoStatement(final DoStatementContext ctx) {
        List<ExpressionSegment> params = new ArrayList<>();
        ctx.expr().forEach(each -> params.add((ExpressionSegment) visit(each)));
        return new MySQLDoStatement(params);
    }
}
