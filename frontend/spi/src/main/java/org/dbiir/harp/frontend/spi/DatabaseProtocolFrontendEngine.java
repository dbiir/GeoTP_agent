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

package org.dbiir.harp.frontend.spi;

import io.netty.channel.Channel;
import org.dbiir.harp.db.protocol.codec.DatabasePacketCodecEngine;
import org.dbiir.harp.frontend.authentication.AuthenticationEngine;
import org.dbiir.harp.utils.common.spi.type.typed.TypedSPI;
import org.dbiir.harp.backend.session.ConnectionSession;
import org.dbiir.harp.frontend.command.CommandExecuteEngine;

/**
 * Database protocol frontend engine.
 */
public interface DatabaseProtocolFrontendEngine extends TypedSPI {
    
    /**
     * Initialize channel.
     * 
     * @param channel channel
     */
    default void initChannel(Channel channel) {
    }
    
    /**
     * Set database version.
     * 
     * @param databaseName database name
     * @param databaseVersion database version
     */
    default void setDatabaseVersion(String databaseName, String databaseVersion) {
    }
    
    /**
     * Get database packet codec engine.
     * 
     * @return database packet codec engine
     */
    DatabasePacketCodecEngine<?> getCodecEngine();

    /**
     * Get authentication engine.
     *
     * @return authentication engine
     */
    AuthenticationEngine getAuthenticationEngine();

    /**
     * Get command execute engine.
     * 
     * @return command execute engine
     */
    CommandExecuteEngine getCommandExecuteEngine();
    
    /**
     * Release resource.
     * 
     * @param connectionSession connection session
     */
    void release(ConnectionSession connectionSession);
    
    /**
     * Handle exception.
     *
     * @param connectionSession connection session
     * @param exception exception
     */
    void handleException(ConnectionSession connectionSession, Exception exception);
}
