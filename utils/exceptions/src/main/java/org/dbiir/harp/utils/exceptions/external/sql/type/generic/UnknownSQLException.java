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

package org.dbiir.harp.utils.exceptions.external.sql.type.generic;

import org.dbiir.harp.utils.exceptions.external.sql.sqlstate.XOpenSQLState;

/**
 * Unknown SQL exception.
 */
public final class UnknownSQLException extends GenericSQLException {
    
    private static final long serialVersionUID = -7357918573504734977L;
    
    public UnknownSQLException(final Exception cause) {
        super(String.format("Unknown exception: %s", cause.getMessage()), cause, XOpenSQLState.GENERAL_ERROR, 0);
    }
}
