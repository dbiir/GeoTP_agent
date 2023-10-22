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

package org.dbiir.harp.utils.binder.segment.select.projection.impl;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.dbiir.harp.utils.binder.segment.select.projection.Projection;
import org.dbiir.harp.utils.common.database.type.DatabaseType;
import org.dbiir.harp.utils.common.database.type.SchemaSupportedDatabaseType;
import org.dbiir.harp.utils.common.enums.AggregationType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Aggregation projection.
 */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class AggregationProjection implements Projection {
    
    private final AggregationType type;
    
    private final String innerExpression;
    
    private final String alias;
    
    private final DatabaseType databaseType;
    
    private final List<AggregationProjection> derivedAggregationProjections = new ArrayList<>(2);
    
    @Setter
    private int index = -1;
    
    @Override
    public final String getExpression() {
        return type.name() + innerExpression;
    }
    
    @Override
    public final Optional<String> getAlias() {
        return Optional.ofNullable(alias);
    }
    
    /**
     * Get column label.
     *
     * @return column label
     */
    @Override
    public String getColumnLabel() {
        return getAlias().orElseGet(() -> databaseType instanceof SchemaSupportedDatabaseType ? type.name().toLowerCase() : getExpression());
    }
    
    @Override
    public Projection cloneWithOwner(final String ownerName) {
        // TODO replace column owner when AggregationProjection contains owner
        AggregationProjection result = new AggregationProjection(type, innerExpression, alias, databaseType);
        result.setIndex(index);
        result.getDerivedAggregationProjections().addAll(derivedAggregationProjections);
        return result;
    }
}
