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

import com.google.common.base.Preconditions;
import lombok.NoArgsConstructor;
import org.dbiir.harp.parser.api.visitor.operation.SQLStatementVisitor;
import org.dbiir.harp.parser.api.visitor.type.DDLSQLVisitor;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AddColumnContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AddTableConstraintContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterCheckContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterConstraintContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterConvertContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterDatabaseContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterEventContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterFunctionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterInstanceContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterListContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterListItemContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterLogfileGroupContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterProcedureContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterRenameTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterServerContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterTableDropContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterTablespaceContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.AlterViewContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.BeginStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CaseStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ChangeColumnContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CharsetNameContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ColumnDefinitionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CompoundStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateDatabaseContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateDefinitionClauseContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateEventContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateFunctionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateIndexContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateLikeClauseContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateLogfileGroupContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateProcedureContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateServerContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateTablespaceContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateTriggerContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.CreateViewContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DeallocateContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropDatabaseContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropEventContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropFunctionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropIndexContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropLogfileGroupContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropProcedureContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropServerContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropTablespaceContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropTriggerContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.DropViewContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ExecuteStmtContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.FieldDefinitionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.FlowControlStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.FunctionNameContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.IfStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.KeyListWithExpressionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.KeyPartContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.KeyPartWithExpressionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.KeyPartsContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.LoopStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ModifyColumnContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.PlaceContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.PrepareContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ReferenceDefinitionContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.RenameIndexContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.RenameTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.RepeatStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.RoutineBodyContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.SimpleStatementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.TableConstraintDefContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.TableElementContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.TableNameContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.TruncateTableContext;
import org.dbiir.harp.parser.type.mysql.autogen.MySQLStatementParser.ValidStatementContext;
import org.dbiir.harp.utils.common.ASTNode;
import org.dbiir.harp.utils.common.segment.ddl.AlterDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.CreateDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.charset.CharsetNameSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.ColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.AddColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.ChangeColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.DropColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.ModifyColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.position.ColumnAfterPositionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.position.ColumnFirstPositionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.position.ColumnPositionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.ConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.ConstraintSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.AddConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.DropConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.ModifyConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.DropIndexDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.IndexNameSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.IndexSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.RenameIndexDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.routine.FunctionNameSegment;
import org.dbiir.harp.utils.common.segment.ddl.routine.RoutineBodySegment;
import org.dbiir.harp.utils.common.segment.ddl.routine.ValidStatementSegment;
import org.dbiir.harp.utils.common.segment.ddl.table.ConvertTableDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.table.RenameTableDefinitionSegment;
import org.dbiir.harp.utils.common.segment.dml.column.ColumnSegment;
import org.dbiir.harp.utils.common.segment.dml.expr.simple.SimpleExpressionSegment;
import org.dbiir.harp.utils.common.segment.generic.DataTypeSegment;
import org.dbiir.harp.utils.common.segment.generic.OwnerSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;
import org.dbiir.harp.utils.common.statement.SQLStatement;
import org.dbiir.harp.utils.common.statement.mysql.ddl.*;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLDeleteStatement;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLInsertStatement;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLSelectStatement;
import org.dbiir.harp.utils.common.statement.mysql.dml.MySQLUpdateStatement;
import org.dbiir.harp.utils.common.value.collection.CollectionValue;
import org.dbiir.harp.utils.common.value.identifier.IdentifierValue;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * DDL Statement SQL visitor for MySQL.
 */
@NoArgsConstructor
public final class MySQLDDLStatementSQLVisitor extends MySQLStatementSQLVisitor implements DDLSQLVisitor, SQLStatementVisitor {
    
    public MySQLDDLStatementSQLVisitor(final Properties props) {
        super(props);
    }
    
    @Override
    public ASTNode visitCreateView(final CreateViewContext ctx) {
        MySQLCreateViewStatement result = new MySQLCreateViewStatement();
        result.setView((SimpleTableSegment) visit(ctx.viewName()));
        result.setViewDefinition(getOriginalText(ctx.select()));
        result.setSelect((MySQLSelectStatement) visit(ctx.select()));
        return result;
    }
    
    @Override
    public ASTNode visitAlterView(final AlterViewContext ctx) {
        MySQLAlterViewStatement result = new MySQLAlterViewStatement();
        result.setView((SimpleTableSegment) visit(ctx.viewName()));
        result.setViewDefinition(getOriginalText(ctx.select()));
        result.setSelect((MySQLSelectStatement) visit(ctx.select()));
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitDropView(final DropViewContext ctx) {
        MySQLDropViewStatement result = new MySQLDropViewStatement();
        result.getViews().addAll(((CollectionValue<SimpleTableSegment>) visit(ctx.viewNames())).getValue());
        return result;
    }
    
    @Override
    public ASTNode visitCreateDatabase(final CreateDatabaseContext ctx) {
        MySQLCreateDatabaseStatement result = new MySQLCreateDatabaseStatement();
        result.setDatabaseName(new IdentifierValue(ctx.schemaName().getText()).getValue());
        result.setIfNotExists(null != ctx.ifNotExists());
        return result;
    }
    
    @Override
    public ASTNode visitAlterDatabase(final AlterDatabaseContext ctx) {
        return new MySQLAlterDatabaseStatement();
    }
    
    @Override
    public ASTNode visitDropDatabase(final DropDatabaseContext ctx) {
        MySQLDropDatabaseStatement result = new MySQLDropDatabaseStatement();
        result.setDatabaseName(new IdentifierValue(ctx.schemaName().getText()).getValue());
        result.setIfExists(null != ctx.ifExists());
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitCreateTable(final CreateTableContext ctx) {
        MySQLCreateTableStatement result = new MySQLCreateTableStatement(null != ctx.ifNotExists());
        result.setTable((SimpleTableSegment) visit(ctx.tableName()));
        if (null != ctx.createDefinitionClause()) {
            CollectionValue<CreateDefinitionSegment> createDefinitions = (CollectionValue<CreateDefinitionSegment>) visit(ctx.createDefinitionClause());
            for (CreateDefinitionSegment each : createDefinitions.getValue()) {
                if (each instanceof ColumnDefinitionSegment) {
                    result.getColumnDefinitions().add((ColumnDefinitionSegment) each);
                } else if (each instanceof ConstraintDefinitionSegment) {
                    result.getConstraintDefinitions().add((ConstraintDefinitionSegment) each);
                }
            }
        }
        return result;
    }
    
    @Override
    public ASTNode visitCreateDefinitionClause(final CreateDefinitionClauseContext ctx) {
        CollectionValue<CreateDefinitionSegment> result = new CollectionValue<>();
        for (TableElementContext each : ctx.tableElementList().tableElement()) {
            if (null != each.columnDefinition()) {
                result.getValue().add((ColumnDefinitionSegment) visit(each.columnDefinition()));
            }
            if (null != each.tableConstraintDef()) {
                result.getValue().add((ConstraintDefinitionSegment) visit(each.tableConstraintDef()));
            }
        }
        return result;
    }
    
    @Override
    public ASTNode visitCreateLikeClause(final CreateLikeClauseContext ctx) {
        return visit(ctx.tableName());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitAlterTable(final AlterTableContext ctx) {
        MySQLAlterTableStatement result = new MySQLAlterTableStatement();
        result.setTable((SimpleTableSegment) visit(ctx.tableName()));
        if (null != ctx.alterTableActions() && null != ctx.alterTableActions().alterCommandList() && null != ctx.alterTableActions().alterCommandList().alterList()) {
            for (AlterDefinitionSegment each : ((CollectionValue<AlterDefinitionSegment>) visit(ctx.alterTableActions().alterCommandList().alterList())).getValue()) {
                if (each instanceof AddColumnDefinitionSegment) {
                    result.getAddColumnDefinitions().add((AddColumnDefinitionSegment) each);
                } else if (each instanceof ModifyColumnDefinitionSegment) {
                    result.getModifyColumnDefinitions().add((ModifyColumnDefinitionSegment) each);
                } else if (each instanceof ChangeColumnDefinitionSegment) {
                    result.getChangeColumnDefinitions().add((ChangeColumnDefinitionSegment) each);
                } else if (each instanceof DropColumnDefinitionSegment) {
                    result.getDropColumnDefinitions().add((DropColumnDefinitionSegment) each);
                } else if (each instanceof AddConstraintDefinitionSegment) {
                    result.getAddConstraintDefinitions().add((AddConstraintDefinitionSegment) each);
                } else if (each instanceof DropConstraintDefinitionSegment) {
                    result.getDropConstraintDefinitions().add((DropConstraintDefinitionSegment) each);
                } else if (each instanceof RenameTableDefinitionSegment) {
                    result.setRenameTable(((RenameTableDefinitionSegment) each).getRenameTable());
                } else if (each instanceof ConvertTableDefinitionSegment) {
                    result.setConvertTableDefinition((ConvertTableDefinitionSegment) each);
                } else if (each instanceof DropIndexDefinitionSegment) {
                    result.getDropIndexDefinitions().add((DropIndexDefinitionSegment) each);
                } else if (each instanceof RenameIndexDefinitionSegment) {
                    result.getRenameIndexDefinitions().add((RenameIndexDefinitionSegment) each);
                }
            }
        }
        return result;
    }
    
    private ColumnDefinitionSegment generateColumnDefinitionSegment(final ColumnSegment column, final FieldDefinitionContext ctx) {
        DataTypeSegment dataTypeSegment = (DataTypeSegment) visit(ctx.dataType());
        boolean isPrimaryKey = ctx.columnAttribute().stream().anyMatch(each -> null != each.KEY() && null == each.UNIQUE());
        // TODO parse not null
        return new ColumnDefinitionSegment(column.getStartIndex(), ctx.getStop().getStopIndex(), column, dataTypeSegment, isPrimaryKey, false);
    }
    
    @Override
    public ASTNode visitAlterConstraint(final AlterConstraintContext ctx) {
        return new ModifyConstraintDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), (ConstraintSegment) visit(ctx.constraintName()));
    }
    
    @Override
    public ASTNode visitAlterList(final AlterListContext ctx) {
        CollectionValue<AlterDefinitionSegment> result = new CollectionValue<>();
        if (ctx.alterListItem().isEmpty()) {
            return result;
        }
        for (AlterListItemContext each : ctx.alterListItem()) {
            if (each instanceof AddColumnContext) {
                result.getValue().add((AddColumnDefinitionSegment) visit(each));
            }
            if (each instanceof AlterConstraintContext || each instanceof AlterCheckContext) {
                result.getValue().add((AlterDefinitionSegment) visit(each));
            }
            if (each instanceof ChangeColumnContext) {
                result.getValue().add(generateModifyColumnDefinitionSegment((ChangeColumnContext) each));
            }
            if (each instanceof ModifyColumnContext) {
                result.getValue().add(generateModifyColumnDefinitionSegment((ModifyColumnContext) each));
            }
            if (each instanceof AlterTableDropContext) {
                AlterTableDropContext alterTableDrop = (AlterTableDropContext) each;
                if (null != alterTableDrop.CHECK() || null != alterTableDrop.CONSTRAINT()) {
                    ConstraintSegment constraintSegment = new ConstraintSegment(alterTableDrop.identifier().getStart().getStartIndex(), alterTableDrop.identifier().getStop().getStopIndex(),
                            (IdentifierValue) visit(alterTableDrop.identifier()));
                    result.getValue().add(new DropConstraintDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), constraintSegment));
                } else if (null == alterTableDrop.KEY() && null == alterTableDrop.keyOrIndex()) {
                    result.getValue().add(generateDropColumnDefinitionSegment(alterTableDrop));
                } else if (null != alterTableDrop.keyOrIndex()) {
                    IndexSegment indexSegment = (IndexSegment) visit(alterTableDrop.indexName());
                    result.getValue().add(new DropIndexDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), indexSegment));
                }
            }
            if (each instanceof AddTableConstraintContext) {
                result.getValue().add((AddConstraintDefinitionSegment) visit(each));
            }
            if (each instanceof AlterRenameTableContext) {
                result.getValue().add((RenameTableDefinitionSegment) visit(each));
            }
            if (each instanceof AlterConvertContext) {
                result.getValue().add((ConvertTableDefinitionSegment) visit(each));
            }
            if (each instanceof RenameIndexContext) {
                result.getValue().add((RenameIndexDefinitionSegment) visit(each));
            }
        }
        return result;
    }
    
    @Override
    public ASTNode visitAlterConvert(final AlterConvertContext ctx) {
        ConvertTableDefinitionSegment result = new ConvertTableDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), (CharsetNameSegment) visit(ctx.charsetName()));
        if (null != ctx.collateClause()) {
            result.setCollateValue((SimpleExpressionSegment) visit(ctx.collateClause()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitCharsetName(final CharsetNameContext ctx) {
        return new CharsetNameSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), ctx.getText());
    }
    
    @Override
    public ASTNode visitAddTableConstraint(final AddTableConstraintContext ctx) {
        return new AddConstraintDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), (ConstraintDefinitionSegment) visit(ctx.tableConstraintDef()));
    }
    
    @Override
    public ASTNode visitAlterCheck(final AlterCheckContext ctx) {
        return new ModifyConstraintDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), (ConstraintSegment) visit(ctx.constraintName()));
    }
    
    @Override
    public ASTNode visitAlterRenameTable(final AlterRenameTableContext ctx) {
        RenameTableDefinitionSegment result = new RenameTableDefinitionSegment(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        result.setRenameTable((SimpleTableSegment) visit(ctx.tableName()));
        return result;
    }
    
    @Override
    public ASTNode visitRenameTable(final RenameTableContext ctx) {
        MySQLRenameTableStatement result = new MySQLRenameTableStatement();
        for (int i = 0, len = ctx.tableName().size(); i < len; i += 2) {
            TableNameContext tableName = ctx.tableName(i);
            TableNameContext renameTableName = ctx.tableName(i + 1);
            result.getRenameTables().add(createRenameTableDefinitionSegment(tableName, renameTableName));
        }
        return result;
    }
    
    private RenameTableDefinitionSegment createRenameTableDefinitionSegment(final TableNameContext tableName, final TableNameContext renameTableName) {
        RenameTableDefinitionSegment result = new RenameTableDefinitionSegment(tableName.start.getStartIndex(), renameTableName.stop.getStopIndex());
        result.setTable((SimpleTableSegment) visit(tableName));
        result.setRenameTable((SimpleTableSegment) visit(renameTableName));
        return result;
    }
    
    private DropColumnDefinitionSegment generateDropColumnDefinitionSegment(final AlterTableDropContext ctx) {
        ColumnSegment column = new ColumnSegment(ctx.columnInternalRef.start.getStartIndex(), ctx.columnInternalRef.stop.getStopIndex(),
                (IdentifierValue) visit(ctx.columnInternalRef));
        return new DropColumnDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), Collections.singletonList(column));
    }
    
    private ModifyColumnDefinitionSegment generateModifyColumnDefinitionSegment(final ModifyColumnContext ctx) {
        ColumnSegment column = new ColumnSegment(ctx.columnInternalRef.start.getStartIndex(), ctx.columnInternalRef.stop.getStopIndex(), (IdentifierValue) visit(ctx.columnInternalRef));
        ModifyColumnDefinitionSegment result = new ModifyColumnDefinitionSegment(
                ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), generateColumnDefinitionSegment(column, ctx.fieldDefinition()));
        if (null != ctx.place()) {
            result.setColumnPosition((ColumnPositionSegment) visit(ctx.place()));
        }
        return result;
    }
    
    private ChangeColumnDefinitionSegment generateModifyColumnDefinitionSegment(final ChangeColumnContext ctx) {
        ChangeColumnDefinitionSegment result = new ChangeColumnDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), (ColumnDefinitionSegment) visit(ctx.columnDefinition()));
        result.setPreviousColumn(new ColumnSegment(ctx.columnInternalRef.getStart().getStartIndex(), ctx.columnInternalRef.getStop().getStopIndex(),
                new IdentifierValue(ctx.columnInternalRef.getText())));
        if (null != ctx.place()) {
            result.setColumnPosition((ColumnPositionSegment) visit(ctx.place()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitAddColumn(final AddColumnContext ctx) {
        Collection<ColumnDefinitionSegment> columnDefinitions = new LinkedList<>();
        if (null != ctx.columnDefinition()) {
            columnDefinitions.add((ColumnDefinitionSegment) visit(ctx.columnDefinition()));
        }
        if (null != ctx.tableElementList()) {
            for (TableElementContext each : ctx.tableElementList().tableElement()) {
                if (null != each.columnDefinition()) {
                    columnDefinitions.add((ColumnDefinitionSegment) visit(each.columnDefinition()));
                }
            }
        }
        AddColumnDefinitionSegment result = new AddColumnDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), columnDefinitions);
        if (null != ctx.place()) {
            Preconditions.checkState(1 == columnDefinitions.size());
            result.setColumnPosition((ColumnPositionSegment) visit(ctx.place()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitColumnDefinition(final ColumnDefinitionContext ctx) {
        ColumnSegment column = new ColumnSegment(ctx.column_name.start.getStartIndex(), ctx.column_name.stop.getStopIndex(), (IdentifierValue) visit(ctx.column_name));
        DataTypeSegment dataTypeSegment = (DataTypeSegment) visit(ctx.fieldDefinition().dataType());
        boolean isPrimaryKey = ctx.fieldDefinition().columnAttribute().stream().anyMatch(each -> null != each.KEY() && null == each.UNIQUE());
        // TODO parse not null
        ColumnDefinitionSegment result = new ColumnDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), column, dataTypeSegment, isPrimaryKey, false);
        result.getReferencedTables().addAll(getReferencedTables(ctx));
        return result;
    }
    
    private Collection<SimpleTableSegment> getReferencedTables(final ColumnDefinitionContext ctx) {
        Collection<SimpleTableSegment> result = new LinkedList<>();
        if (null != ctx.referenceDefinition()) {
            result.add((SimpleTableSegment) visit(ctx.referenceDefinition()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitTableConstraintDef(final TableConstraintDefContext ctx) {
        ConstraintDefinitionSegment result = new ConstraintDefinitionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex());
        if (null != ctx.constraintClause() && null != ctx.constraintClause().constraintName()) {
            result.setConstraintName((ConstraintSegment) visit(ctx.constraintClause().constraintName()));
        }
        if (null != ctx.KEY() && null != ctx.PRIMARY()) {
            result.getPrimaryKeyColumns().addAll(((CollectionValue) visit(ctx.keyListWithExpression())).getValue());
            return result;
        }
        if (null != ctx.FOREIGN()) {
            result.setReferencedTable((SimpleTableSegment) visit(ctx.referenceDefinition()));
            return result;
        }
        if (null != ctx.UNIQUE()) {
            result.getIndexColumns().addAll(((CollectionValue) visit(ctx.keyListWithExpression())).getValue());
            if (null != ctx.indexName()) {
                result.setIndexName((IndexSegment) visit(ctx.indexName()));
            }
            return result;
        }
        if (null != ctx.checkConstraint()) {
            return result;
        }
        result.getIndexColumns().addAll(((CollectionValue) visit(ctx.keyListWithExpression())).getValue());
        if (null != ctx.indexName()) {
            result.setIndexName((IndexSegment) visit(ctx.indexName()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitKeyListWithExpression(final KeyListWithExpressionContext ctx) {
        CollectionValue<ColumnSegment> result = new CollectionValue<>();
        for (KeyPartWithExpressionContext each : ctx.keyPartWithExpression()) {
            if (null != each.keyPart()) {
                result.getValue().add((ColumnSegment) visit(each.keyPart().columnName()));
            }
        }
        return result;
    }
    
    @Override
    public ASTNode visitReferenceDefinition(final ReferenceDefinitionContext ctx) {
        return visit(ctx.tableName());
    }
    
    @Override
    public ASTNode visitPlace(final PlaceContext ctx) {
        ColumnSegment columnName = null;
        if (null != ctx.columnName()) {
            columnName = (ColumnSegment) visit(ctx.columnName());
        }
        return null == ctx.columnName()
                ? new ColumnFirstPositionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), columnName)
                : new ColumnAfterPositionSegment(ctx.getStart().getStartIndex(), ctx.getStop().getStopIndex(), columnName);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitDropTable(final DropTableContext ctx) {
        MySQLDropTableStatement result = new MySQLDropTableStatement(null != ctx.ifExists());
        result.getTables().addAll(((CollectionValue<SimpleTableSegment>) visit(ctx.tableList())).getValue());
        return result;
    }
    
    @Override
    public ASTNode visitTruncateTable(final TruncateTableContext ctx) {
        MySQLTruncateStatement result = new MySQLTruncateStatement();
        result.getTables().add((SimpleTableSegment) visit(ctx.tableName()));
        return result;
    }
    
    @Override
    public ASTNode visitCreateIndex(final CreateIndexContext ctx) {
        MySQLCreateIndexStatement result = new MySQLCreateIndexStatement();
        result.setTable((SimpleTableSegment) visit(ctx.tableName()));
        IndexNameSegment indexName = new IndexNameSegment(ctx.indexName().start.getStartIndex(), ctx.indexName().stop.getStopIndex(), new IdentifierValue(ctx.indexName().getText()));
        result.setIndex(new IndexSegment(ctx.indexName().start.getStartIndex(), ctx.indexName().stop.getStopIndex(), indexName));
        result.getColumns().addAll(((CollectionValue) visit(ctx.keyListWithExpression())).getValue());
        return result;
    }
    
    @Override
    public ASTNode visitDropIndex(final DropIndexContext ctx) {
        MySQLDropIndexStatement result = new MySQLDropIndexStatement();
        result.setTable((SimpleTableSegment) visit(ctx.tableName()));
        IndexNameSegment indexName = new IndexNameSegment(ctx.indexName().start.getStartIndex(), ctx.indexName().stop.getStopIndex(), new IdentifierValue(ctx.indexName().getText()));
        result.getIndexes().add(new IndexSegment(ctx.indexName().start.getStartIndex(), ctx.indexName().stop.getStopIndex(), indexName));
        return result;
    }
    
    @Override
    public ASTNode visitRenameIndex(final RenameIndexContext ctx) {
        IndexSegment indexNameSegment = (IndexSegment) visitIndexName(ctx.indexName(0));
        IndexSegment renameIndexName = (IndexSegment) visitIndexName(ctx.indexName(1));
        return new RenameIndexDefinitionSegment(ctx.start.getStartIndex(), ctx.stop.getStopIndex(), indexNameSegment, renameIndexName);
    }
    
    @Override
    public ASTNode visitKeyParts(final KeyPartsContext ctx) {
        CollectionValue<ColumnSegment> result = new CollectionValue<>();
        List<KeyPartContext> keyParts = ctx.keyPart();
        for (KeyPartContext each : keyParts) {
            if (null != each.columnName()) {
                result.getValue().add((ColumnSegment) visit(each.columnName()));
            }
        }
        return result;
    }
    
    @Override
    public ASTNode visitCreateProcedure(final CreateProcedureContext ctx) {
        MySQLCreateProcedureStatement result = new MySQLCreateProcedureStatement();
        result.setProcedureName((FunctionNameSegment) visit(ctx.functionName()));
        result.setRoutineBody((RoutineBodySegment) visit(ctx.routineBody()));
        return result;
    }
    
    @Override
    public ASTNode visitFunctionName(final FunctionNameContext ctx) {
        FunctionNameSegment result = new FunctionNameSegment(ctx.start.getStartIndex(), ctx.stop.getStopIndex(), (IdentifierValue) visit(ctx.identifier()));
        if (null != ctx.owner()) {
            result.setOwner((OwnerSegment) visit(ctx.owner()));
        }
        return result;
    }
    
    @Override
    public ASTNode visitAlterProcedure(final AlterProcedureContext ctx) {
        return new MySQLAlterProcedureStatement();
    }
    
    @Override
    public ASTNode visitDropProcedure(final DropProcedureContext ctx) {
        return new MySQLDropProcedureStatement();
    }
    
    @Override
    public ASTNode visitCreateFunction(final CreateFunctionContext ctx) {
        MySQLCreateFunctionStatement result = new MySQLCreateFunctionStatement();
        result.setFunctionName((FunctionNameSegment) visit(ctx.functionName()));
        result.setRoutineBody((RoutineBodySegment) visit(ctx.routineBody()));
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitRoutineBody(final RoutineBodyContext ctx) {
        RoutineBodySegment result = new RoutineBodySegment(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        CollectionValue<ValidStatementSegment> validStatements = null == ctx.simpleStatement()
                ? (CollectionValue<ValidStatementSegment>) visit(ctx.compoundStatement())
                : (CollectionValue<ValidStatementSegment>) visit(ctx.simpleStatement());
        result.getValidStatements().addAll(validStatements.getValue());
        return result;
    }
    
    @Override
    public ASTNode visitSimpleStatement(final SimpleStatementContext ctx) {
        return visit(ctx.validStatement());
    }
    
    @Override
    public ASTNode visitCompoundStatement(final CompoundStatementContext ctx) {
        return visit(ctx.beginStatement());
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitBeginStatement(final BeginStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitValidStatement(final ValidStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        ValidStatementSegment validStatement = createValidStatementSegment(ctx);
        if (null != validStatement.getSqlStatement()) {
            result.getValue().add(validStatement);
        }
        if (null != ctx.beginStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.beginStatement()));
        }
        if (null != ctx.flowControlStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.flowControlStatement()));
        }
        return result;
    }
    
    private ValidStatementSegment createValidStatementSegment(final ValidStatementContext ctx) {
        ValidStatementSegment result = new ValidStatementSegment(ctx.start.getStartIndex(), ctx.stop.getStopIndex());
        SQLStatement sqlStatement = null;
        if (null != ctx.createTable()) {
            sqlStatement = (MySQLCreateTableStatement) visit(ctx.createTable());
        } else if (null != ctx.alterTable()) {
            sqlStatement = (MySQLAlterTableStatement) visit(ctx.alterTable());
        } else if (null != ctx.dropTable()) {
            sqlStatement = (MySQLDropTableStatement) visit(ctx.dropTable());
        } else if (null != ctx.truncateTable()) {
            sqlStatement = (MySQLTruncateStatement) visit(ctx.truncateTable());
        } else if (null != ctx.insert()) {
            sqlStatement = (MySQLInsertStatement) visit(ctx.insert());
        } else if (null != ctx.replace()) {
            sqlStatement = (MySQLInsertStatement) visit(ctx.replace());
        } else if (null != ctx.update()) {
            sqlStatement = (MySQLUpdateStatement) visit(ctx.update());
        } else if (null != ctx.delete()) {
            sqlStatement = (MySQLDeleteStatement) visit(ctx.delete());
        } else if (null != ctx.select()) {
            sqlStatement = (MySQLSelectStatement) visit(ctx.select());
        }
        result.setSqlStatement(sqlStatement);
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitFlowControlStatement(final FlowControlStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        if (null != ctx.caseStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.caseStatement()));
        }
        if (null != ctx.ifStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.ifStatement()));
        }
        if (null != ctx.loopStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.loopStatement()));
        }
        if (null != ctx.repeatStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.repeatStatement()));
        }
        if (null != ctx.whileStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(ctx.whileStatement()));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitCaseStatement(final CaseStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitIfStatement(final IfStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitLoopStatement(final LoopStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitRepeatStatement(final RepeatStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ASTNode visitWhileStatement(final MySQLStatementParser.WhileStatementContext ctx) {
        CollectionValue<ValidStatementSegment> result = new CollectionValue<>();
        for (ValidStatementContext each : ctx.validStatement()) {
            result.combine((CollectionValue<ValidStatementSegment>) visit(each));
        }
        return result;
    }
    
    @Override
    public ASTNode visitAlterFunction(final AlterFunctionContext ctx) {
        return new MySQLAlterFunctionStatement();
    }
    
    @Override
    public ASTNode visitDropFunction(final DropFunctionContext ctx) {
        return new MySQLDropFunctionStatement();
    }
    
    @Override
    public ASTNode visitCreateEvent(final CreateEventContext ctx) {
        return new MySQLCreateEventStatement();
    }
    
    @Override
    public ASTNode visitAlterEvent(final AlterEventContext ctx) {
        return new MySQLAlterEventStatement();
    }
    
    @Override
    public ASTNode visitDropEvent(final DropEventContext ctx) {
        return new MySQLDropEventStatement();
    }
    
    @Override
    public ASTNode visitAlterInstance(final AlterInstanceContext ctx) {
        return new MySQLAlterInstanceStatement();
    }
    
    @Override
    public ASTNode visitCreateLogfileGroup(final CreateLogfileGroupContext ctx) {
        return new MySQLCreateLogfileGroupStatement();
    }
    
    @Override
    public ASTNode visitAlterLogfileGroup(final AlterLogfileGroupContext ctx) {
        return new MySQLAlterLogfileGroupStatement();
    }
    
    @Override
    public ASTNode visitDropLogfileGroup(final DropLogfileGroupContext ctx) {
        return new MySQLDropLogfileGroupStatement();
    }
    
    @Override
    public ASTNode visitCreateServer(final CreateServerContext ctx) {
        return new MySQLCreateServerStatement();
    }
    
    @Override
    public ASTNode visitAlterServer(final AlterServerContext ctx) {
        return new MySQLAlterServerStatement();
    }
    
    @Override
    public ASTNode visitDropServer(final DropServerContext ctx) {
        return new MySQLDropServerStatement();
    }
    
    @Override
    public ASTNode visitCreateTrigger(final CreateTriggerContext ctx) {
        return new MySQLCreateTriggerStatement();
    }
    
    @Override
    public ASTNode visitDropTrigger(final DropTriggerContext ctx) {
        return new MySQLDropTriggerStatement();
    }
    
    @Override
    public ASTNode visitCreateTablespace(final CreateTablespaceContext ctx) {
        return new MySQLCreateTablespaceStatement();
    }
    
    @Override
    public ASTNode visitAlterTablespace(final AlterTablespaceContext ctx) {
        return new MySQLAlterTablespaceStatement();
    }
    
    @Override
    public ASTNode visitDropTablespace(final DropTablespaceContext ctx) {
        return new MySQLDropTablespaceStatement();
    }
    
    @Override
    public ASTNode visitPrepare(final PrepareContext ctx) {
        return new MySQLPrepareStatement();
    }
    
    @Override
    public ASTNode visitExecuteStmt(final ExecuteStmtContext ctx) {
        return new MySQLExecuteStatement();
    }
    
    @Override
    public ASTNode visitDeallocate(final DeallocateContext ctx) {
        return new MySQLDeallocateStatement();
    }
}
