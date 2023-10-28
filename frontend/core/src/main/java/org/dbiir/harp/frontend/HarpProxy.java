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

package org.dbiir.harp.frontend;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.dbiir.harp.backend.context.BackendExecutorContext;
import org.dbiir.harp.frontend.async.AgentAsyncPrepare;
import org.dbiir.harp.frontend.async.AgentAsyncSendingThread;
import org.dbiir.harp.frontend.netty.AsyncMessageHandlerInitializer;
import org.dbiir.harp.frontend.netty.ServerHandlerInitializer;
import org.dbiir.harp.frontend.protocol.FrontDatabaseProtocolTypeFactory;
import org.dbiir.harp.utils.transcation.AgentAsyncThreadCollector;
import org.dbiir.harp.utils.transcation.AgentAsyncXAManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Proxy.
 */
@Slf4j
public final class HarpProxy {
    
    private EventLoopGroup bossGroup;   // client connection
    
    private EventLoopGroup workerGroup;

    private EventLoopGroup asyncBossGroup;

    private EventLoopGroup asyncWorkerGroup;


    public HarpProxy() {
        createEventLoopGroup();
        createAuxiliaryThreads();
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
    
    /**
     * Start Proxy.
     *
     * @param port      port
     * @param addresses addresses
     */
    @SneakyThrows(InterruptedException.class)
    public void start(final int port, final List<String> addresses) {
        try {
            List<ChannelFuture> futures = startInternal(port, addresses);
            accept(futures);
        } finally {
            close();
        }
    }
    
    /**
     * Start Proxy with DomainSocket.
     *
     * @param socketPath socket path
     */
    public void start(final String socketPath) {
        if (!Epoll.isAvailable()) {
            log.error("Epoll is unavailable, DomainSocket can't start.");
            return;
        }
        ChannelFuture future = startDomainSocket(socketPath);
        future.addListener((ChannelFutureListener) futureParams -> {
            if (futureParams.isSuccess()) {
                log.info("The listening address for DomainSocket is {}", socketPath);
            } else {
                log.error("DomainSocket failed to start:{}", futureParams.cause().getMessage());
                futureParams.cause().printStackTrace();
            }
        });
    }
    
    private List<ChannelFuture> startInternal(final int port, final List<String> addresses) throws InterruptedException {
        ServerBootstrap bootstrap = new ServerBootstrap();
        initServerBootstrap(bootstrap);
        List<ChannelFuture> futures = new ArrayList<>();
        for (String address : addresses) {
            System.out.println("address: " + address);
            futures.add(bootstrap.bind(address, port).sync());
        }

        Thread thread = new Thread(() -> {
            try {
                ServerBootstrap asyncBootstrap = new ServerBootstrap();
                initAsyncServerBootstrap(asyncBootstrap);
                asyncBootstrap.bind(3308).sync().channel().closeFuture().sync();
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });

        thread.start();

        return futures;
    }

    private void initAsyncServerBootstrap(final ServerBootstrap bootstrap) {
        bootstrap.group(asyncBossGroup, asyncWorkerGroup)
                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024 * 1024, 16 * 1024 * 1024))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new AsyncMessageHandlerInitializer());
    }

    private ChannelFuture startDomainSocket(final String socketPath) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        initServerBootstrap(bootstrap, new DomainSocketAddress(socketPath));
        return bootstrap.bind();
    }
    
    private void accept(final List<ChannelFuture> futures) throws InterruptedException {
        for (ChannelFuture future : futures) {
            future.channel().closeFuture().sync();
        }
    }

    private void createAuxiliaryThreads() {
        AgentAsyncThreadCollector agentAsyncThreadCollector = new AgentAsyncThreadCollector();
        Thread thread1 = new Thread(agentAsyncThreadCollector);
        thread1.start();
        AgentAsyncSendingThread agentAsyncSendingThread = new AgentAsyncSendingThread();
        Thread thread2 = new Thread(agentAsyncSendingThread);
        thread2.start();
    }
    
    private void createEventLoopGroup() {
        bossGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        workerGroup = getWorkerGroup();
        asyncBossGroup = Epoll.isAvailable() ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        asyncWorkerGroup = getWorkerGroup();
    }
    
    private EventLoopGroup getWorkerGroup() {
        int workerThreads = 0;
        return Epoll.isAvailable() ? new EpollEventLoopGroup(workerThreads) : new NioEventLoopGroup(workerThreads);
    }
    
    private void initServerBootstrap(final ServerBootstrap bootstrap) {
        bootstrap.group(bossGroup, workerGroup)
                .channel(Epoll.isAvailable() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(8 * 1024 * 1024, 16 * 1024 * 1024))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ServerHandlerInitializer(FrontDatabaseProtocolTypeFactory.getDatabaseType()));
    }
    
    private void initServerBootstrap(final ServerBootstrap bootstrap, final DomainSocketAddress localDomainSocketAddress) {
        bootstrap.group(bossGroup, workerGroup)
                .channel(EpollServerDomainSocketChannel.class)
                .localAddress(localDomainSocketAddress)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ServerHandlerInitializer(FrontDatabaseProtocolTypeFactory.getDatabaseType()));
    }
    
    private void close() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        asyncBossGroup.shutdownGracefully();
        asyncWorkerGroup.shutdownGracefully();
        BackendExecutorContext.getInstance().getExecutorEngine().close();
    }
}
