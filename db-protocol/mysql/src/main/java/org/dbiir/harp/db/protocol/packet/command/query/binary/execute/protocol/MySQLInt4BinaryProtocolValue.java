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

package org.dbiir.harp.db.protocol.packet.command.query.binary.execute.protocol;

import org.dbiir.harp.db.protocol.payload.MySQLPacketPayload;

import java.math.BigDecimal;

/**
 * Binary protocol value for int4 for MySQL.
 */
public final class MySQLInt4BinaryProtocolValue implements MySQLBinaryProtocolValue {
    
    @SuppressWarnings("SimplifiableIfStatement")
    @Override
    public Object read(final MySQLPacketPayload payload, final boolean unsigned) {
        if (unsigned) {
            return payload.getByteBuf().readUnsignedIntLE();
        }
        return payload.getByteBuf().readIntLE();
    }
    
    @Override
    public void write(final MySQLPacketPayload payload, final Object value) {
        if (value instanceof BigDecimal) {
            payload.writeInt4(((BigDecimal) value).intValue());
        } else if (value instanceof Integer) {
            payload.writeInt4((Integer) value);
        } else {
            payload.writeInt4(((Long) value).intValue());
        }
    }
}
