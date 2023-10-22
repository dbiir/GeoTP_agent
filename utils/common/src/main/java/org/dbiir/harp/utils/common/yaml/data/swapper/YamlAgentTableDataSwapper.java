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

package org.dbiir.harp.utils.common.yaml.data.swapper;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.utils.common.metadata.data.AgentTableData;
import org.dbiir.harp.utils.common.metadata.database.schema.model.AgentColumn;
import org.dbiir.harp.utils.common.yaml.data.pojo.YamlAgentRowData;
import org.dbiir.harp.utils.common.yaml.data.pojo.YamlAgentTableData;
import org.dbiir.harp.utils.common.yaml.swapper.YamlConfigurationSwapper;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * YAML Agent data swapper.
 */
@RequiredArgsConstructor
public final class YamlAgentTableDataSwapper implements YamlConfigurationSwapper<YamlAgentTableData, AgentTableData> {
    
    private final List<AgentColumn> columns;
    
    @Override
    public YamlAgentTableData swapToYamlConfiguration(final AgentTableData data) {
        YamlAgentTableData result = new YamlAgentTableData();
        result.setName(data.getName());
        Collection<YamlAgentRowData> yamlAgentRowData = new LinkedList<>();
        data.getRows().forEach(rowData -> yamlAgentRowData.add(new YamlAgentRowDataSwapper(columns).swapToYamlConfiguration(rowData)));
        result.setRowData(yamlAgentRowData);
        return result;
    }
    
    @Override
    public AgentTableData swapToObject(final YamlAgentTableData yamlConfig) {
        AgentTableData result = new AgentTableData(yamlConfig.getName());
        if (null != yamlConfig.getRowData()) {
            yamlConfig.getRowData().forEach(yamlRowData -> result.getRows().add(new YamlAgentRowDataSwapper(columns).swapToObject(yamlRowData)));
        }
        return result;
    }
}
