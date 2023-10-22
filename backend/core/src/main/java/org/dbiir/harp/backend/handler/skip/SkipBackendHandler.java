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

package org.dbiir.harp.backend.handler.skip;

import lombok.RequiredArgsConstructor;
import org.dbiir.harp.backend.response.header.ResponseHeader;
import org.dbiir.harp.backend.response.header.update.UpdateResponseHeader;
import org.dbiir.harp.backend.handler.ProxyBackendHandler;
import org.dbiir.harp.utils.common.statement.SQLStatement;

import java.util.LinkedList;
import java.util.List;

/**
 * Skip backend handler.
 */
@RequiredArgsConstructor
public final class SkipBackendHandler implements ProxyBackendHandler {
    
    private final SQLStatement sqlStatement;
    
    @Override
    public List<ResponseHeader> execute() {
        List<ResponseHeader> result = new LinkedList<ResponseHeader>();
        result.add(new UpdateResponseHeader(sqlStatement));
        return result;
    }
}
