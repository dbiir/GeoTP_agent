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

package org.dbiir.harp.utils.common.statement.ddl;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.AddColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.ChangeColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.DropColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.column.alter.ModifyColumnDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.AddConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.DropConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.ModifyConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.constraint.alter.ValidateConstraintDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.DropIndexDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.index.RenameIndexDefinitionSegment;
import org.dbiir.harp.utils.common.segment.ddl.table.ConvertTableDefinitionSegment;
import org.dbiir.harp.utils.common.segment.generic.table.SimpleTableSegment;
import org.dbiir.harp.utils.common.statement.AbstractSQLStatement;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;

/**
 * Alter table statement.
 */
@Getter
@Setter
public abstract class AlterTableStatement extends AbstractSQLStatement implements DDLStatement {
    
    private SimpleTableSegment table;
    
    private SimpleTableSegment renameTable;
    
    private ConvertTableDefinitionSegment convertTableDefinition;
    
    private final Collection<AddColumnDefinitionSegment> addColumnDefinitions = new LinkedList<>();
    
    private final Collection<ModifyColumnDefinitionSegment> modifyColumnDefinitions = new LinkedList<>();
    
    private final Collection<ChangeColumnDefinitionSegment> changeColumnDefinitions = new LinkedList<>();
    
    private final Collection<DropColumnDefinitionSegment> dropColumnDefinitions = new LinkedList<>();
    
    private final Collection<AddConstraintDefinitionSegment> addConstraintDefinitions = new LinkedList<>();
    
    private final Collection<ValidateConstraintDefinitionSegment> validateConstraintDefinitions = new LinkedList<>();
    
    private final Collection<ModifyConstraintDefinitionSegment> modifyConstraintDefinitions = new LinkedList<>();
    
    private final Collection<DropConstraintDefinitionSegment> dropConstraintDefinitions = new LinkedList<>();
    
    private final Collection<DropIndexDefinitionSegment> dropIndexDefinitions = new LinkedList<>();
    
    private final Collection<RenameIndexDefinitionSegment> renameIndexDefinitions = new LinkedList<>();
    
    /**
     * Get rename table.
     *
     * @return rename table
     */
    public Optional<SimpleTableSegment> getRenameTable() {
        return Optional.ofNullable(renameTable);
    }
    
    /**
     * Get convert table definition.
     *
     * @return convert table definition
     */
    public Optional<ConvertTableDefinitionSegment> getConvertTableDefinition() {
        return Optional.ofNullable(convertTableDefinition);
    }
}
