/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.memorynbdserver;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import java.net.InetSocketAddress;
import py.common.struct.EndPoint;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.NbdByteToMessageDecoder;
import py.coordinator.nbd.PydClientManager;

public class MemoryNbdServer {

  private final long memorySize;
  private final String ipAddress;
  private final int port;
  private EventLoopGroup ioEventGroup;
  private EventLoopGroup processEventGroup;
  private ChannelFuture serverChannelFuture;
  private PydClientManager pydClientManager;
  private int ioDepth;
  private EventExecutorGroup eventExecutorGroup;
  private StorageDriver storageDriver;

  /**
   * xx.
   */
  public MemoryNbdServer(long memorySize, String ipAddress, int port, int ioDepth,
      StorageDriver storageDriver) {
    this.memorySize = memorySize;
    this.ipAddress = ipAddress;
    this.port = port;
    this.ioDepth = ioDepth;
    this.ioEventGroup = new NioEventLoopGroup(1,
        new DefaultThreadFactory("memory-nbd-server", false, Thread.NORM_PRIORITY));
    this.processEventGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
        new DefaultThreadFactory("io-processor", false, Thread.NORM_PRIORITY));
    this.eventExecutorGroup = new DefaultEventExecutorGroup(
        Runtime.getRuntime().availableProcessors());
    this.storageDriver = storageDriver;
    this.pydClientManager = new PydClientManager(0, true, 30, storageDriver);
  }

  /**
   * xx.
   */
  public void start() throws Exception {
    EndPoint point = new EndPoint(ipAddress, port);
    ByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    ServerBootstrap bootstrap = new ServerBootstrap();

    bootstrap.group(ioEventGroup, this.processEventGroup).channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_BACKLOG, 256)
        .childOption(ChannelOption.TCP_NODELAY, true).childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_SNDBUF, 1024 * 1024 * 10)
        .childOption(ChannelOption.WRITE_SPIN_COUNT, 50)
        .childOption(ChannelOption.ALLOCATOR, allocator)
        .childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(512, 8192, 131072))
        .childHandler(new ChannelInitializer<SocketChannel>() {

          @Override
          protected void initChannel(SocketChannel ch) throws Exception {

            MemoryNbdConnectionHandler nbdConnectionHandler = new MemoryNbdConnectionHandler(
                memorySize);
            nbdConnectionHandler.setPydClientManager(pydClientManager);
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(nbdConnectionHandler);

            pipeline.addLast("IdleStateHandler", new IdleStateHandler(30, 30, 30));
            MemNbdIdleStateHandler nbdIdleStateHandler = new MemNbdIdleStateHandler();
            nbdIdleStateHandler.setPydClientManager(pydClientManager);
            pipeline.addLast("MemNBDIdleStateHandler", nbdIdleStateHandler);

            // initialize instance of frame decoder
            NbdByteToMessageDecoder decoder = new NbdByteToMessageDecoder();
            decoder.setMaxFrameSize(16 * 1024 * 1024); // 16M
            pipeline.addLast(decoder);

            // add dispatcher for sending io requests to datanodes.
            MemoryNbdProcessor nbdProcessor = new MemoryNbdProcessor(memorySize, ioDepth,
                pydClientManager, storageDriver);
            nbdProcessor.setEventExecutorGroup(eventExecutorGroup);
            // pipeline.addLast(eventExecutorGroup, "memory-processors", nbdProcessor);
            pipeline.addLast(nbdProcessor);
          }
        });
    this.serverChannelFuture = bootstrap
        .bind(new InetSocketAddress(point.getHostName(), point.getPort())).sync();
  }

  public int getPort() {
    return port;
  }

}
