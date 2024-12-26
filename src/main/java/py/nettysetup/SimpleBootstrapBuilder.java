/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.nettysetup;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.connection.pool.AbstractBootstrapBuilder;
import py.netty.client.TransferenceClientOption;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.netty.message.SendMessage;

public class SimpleBootstrapBuilder extends AbstractBootstrapBuilder {

  private static final Logger logger = LoggerFactory.getLogger(SimpleBootstrapBuilder.class);
  private final Semaphore semaphore;

  public SimpleBootstrapBuilder(Semaphore semaphore) {
    this.semaphore = semaphore;
  }

  @Override
  public Bootstrap build() {
    ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();

        // pipeline.addLast("framer", new FixedLengthFrameDecoder(1));
//        p.addLast("decoder", new ClientDecoder()); //in
        p.addLast(new SimpleNettyClientOutboundHandler());
        p.addLast("handler", new SimpleNettyClientInboundHandler()); //in
      }
    };

    Bootstrap bootstrap = new Bootstrap();

    int connectionTimeout = (int) cfg.valueOf(TransferenceClientOption.IO_CONNECTION_TIMEOUT_MS);
    bootstrap.group(ioEventGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) connectionTimeout).handler(initializer);
    bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocatorWrapper.INSTANCE);
    bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR,
        new AdaptiveRecvByteBufAllocator(512, 8192, 128 * 1024));
    return bootstrap;
  }

  /**
   * xx.
   */
  public class SimpleNettyClientOutboundHandler extends ChannelOutboundHandlerAdapter {

    private final int predefinedLength = 8192;
    private final byte[] data = new byte[predefinedLength];

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      SendMessage message = (SendMessage) msg;
      ByteBuf byteBuf = message.getBuffer();

      if (byteBuf.readableBytes() != predefinedLength) {
        logger.info("the readable bytes is not predefined {} ", byteBuf.readableBytes());
        byteBuf = Unpooled.wrappedBuffer(data);
      }

      ctx.write(byteBuf, promise);
    }
  }

  /**
   * xx.
   */
  public class SimpleNettyClientInboundHandler extends SimpleChannelInboundHandler<byte[]> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
      // System.out.println(ctx.channel().remoteAddress() + "client receive: " + msg.length);
      semaphore.release(msg.length);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      System.out.println("Client active ");
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      System.out.println("Client close recvConunt:");
      super.channelInactive(ctx);
    }
  }
}
