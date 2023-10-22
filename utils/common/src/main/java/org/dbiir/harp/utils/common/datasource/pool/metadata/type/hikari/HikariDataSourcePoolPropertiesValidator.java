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

package org.dbiir.harp.utils.common.datasource.pool.metadata.type.hikari;


import org.dbiir.harp.utils.common.datasource.pool.metadata.DataSourcePoolPropertiesValidator;
import org.dbiir.harp.utils.common.datasource.props.DataSourceProperties;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Hikari data source pool properties validator.
 */
public final class HikariDataSourcePoolPropertiesValidator implements DataSourcePoolPropertiesValidator {
    
    private static final long CONNECTION_TIMEOUT_FLOOR = 250L;
    
    private static final long MAX_LIFETIME_FLOOR = TimeUnit.SECONDS.toMillis(30);
    
    private static final long KEEP_ALIVE_TIME_FLOOR = TimeUnit.SECONDS.toMillis(30);
    
    @Override
    public void validateProperties(final DataSourceProperties dataSourceProps) throws IllegalArgumentException {
        validateConnectionTimeout(dataSourceProps);
        validateIdleTimeout(dataSourceProps);
        validateMaxLifetime(dataSourceProps);
        validateMaximumPoolSize(dataSourceProps);
        validateMinimumIdle(dataSourceProps);
        validateKeepaliveTime(dataSourceProps);
    }
    
    private void validateConnectionTimeout(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "connectionTimeout")) {
            return;
        }
    }
    
    private void validateIdleTimeout(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "idleTimeout")) {
            return;
        }
    }
    
    private void validateMaxLifetime(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "maxLifetime")) {
            return;
        }
    }
    
    private void validateMaximumPoolSize(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "maximumPoolSize")) {
            return;
        }
        int maximumPoolSize = Integer.parseInt(dataSourceProps.getAllLocalProperties().get("maximumPoolSize").toString());
    }
    
    private void validateMinimumIdle(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "minimumIdle")) {
            return;
        }
        int minimumIdle = Integer.parseInt(dataSourceProps.getAllLocalProperties().get("minimumIdle").toString());
    }
    
    private void validateKeepaliveTime(final DataSourceProperties dataSourceProps) {
        if (!checkValueExist(dataSourceProps, "keepaliveTime")) {
            return;
        }
        int keepaliveTime = Integer.parseInt(dataSourceProps.getAllLocalProperties().get("keepaliveTime").toString());
        if (keepaliveTime == 0) {
            return;
        }
    }
    
    private boolean checkValueExist(final DataSourceProperties dataSourceProps, final String key) {
        return dataSourceProps.getAllLocalProperties().containsKey(key) && Objects.nonNull(dataSourceProps.getAllLocalProperties().get(key));
    }
}
