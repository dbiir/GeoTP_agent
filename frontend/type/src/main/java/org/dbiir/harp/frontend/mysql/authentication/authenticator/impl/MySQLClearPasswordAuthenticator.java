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

package org.dbiir.harp.frontend.mysql.authentication.authenticator.impl;

import com.google.common.base.Strings;
import org.dbiir.harp.db.protocol.constant.AuthenticationMethod;
import org.dbiir.harp.db.protocol.constant.MySQLAuthenticationMethod;
import org.dbiir.harp.frontend.mysql.authentication.authenticator.MySQLAuthenticator;
import org.dbiir.harp.utils.common.metadata.user.AgentUser;

/**
 * Clear password authenticator for MySQL.
 * 
 * @see <a href="https://dev.mysql.com/doc/internals/en/clear-text-authentication.html">Clear Text Authentication</a>
 */
public final class MySQLClearPasswordAuthenticator implements MySQLAuthenticator {
    
    @Override
    public boolean authenticate(final AgentUser user, final Object[] authInfo) {
        byte[] authResponse = (byte[]) authInfo[0];
        byte[] password = new byte[authResponse.length - 1];
        System.arraycopy(authResponse, 0, password, 0, authResponse.length - 1);
        return Strings.isNullOrEmpty(user.getPassword()) || user.getPassword().equals(new String(password));
    }
    
    @Override
    public AuthenticationMethod getAuthenticationMethod() {
        return MySQLAuthenticationMethod.CLEAR_TEXT;
    }
}
