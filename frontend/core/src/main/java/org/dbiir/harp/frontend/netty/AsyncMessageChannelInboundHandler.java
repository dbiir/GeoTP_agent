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

package org.dbiir.harp.frontend.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;

/**
 * Frontend channel inbound handler.
 */
@Slf4j
public final class AsyncMessageChannelInboundHandler extends ChannelInboundHandlerAdapter {
    private static ChannelHandlerContext context;
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // initialize the context when the connection is established
        context = ctx;
//        String message = "Hello, server!";
//        ctx.writeAndFlush(message);
    }

    public static void sendMessage(String message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(message).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    public static void sendMessage(byte[] message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(Unpooled.wrappedBuffer(message)).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    public static void sendMessage(ByteBuf message) throws InterruptedException {
        if (context != null) {
            context.writeAndFlush(message).sync();
        } else {
            System.err.println("ChannelHandlerContext is not initialized.");
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        log.info("Received from client: " + msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}
