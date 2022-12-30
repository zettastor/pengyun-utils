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

package py.helloworld;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutorGroup;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.netty.datanode.PyWriteRequest;
import py.nettysetup.SimpleWriteRequestBuilder;

/**
 * xx.
 */
public class HelloClient {

  public static final Semaphore ioDepth = new Semaphore(128);
  static final EventExecutorGroup netty_client_sender = new DefaultEventExecutorGroup(
      Runtime.getRuntime().availableProcessors(),
      new DefaultThreadFactory("netty-client-sender", false, Thread.NORM_PRIORITY));
  private static final Logger logger = LoggerFactory.getLogger(HelloClient.class);
  public static String host = "127.0.0.1";
  public static int port = 7878;

  /**
   * xx.
   */
  public static void main(String[] args) throws InterruptedException, IOException {
    int length = 8 * 1024;
    boolean useThreadPool = false;
    boolean sendBytes = true;
    if (args.length >= 4) {
      host = args[0];
      port = Integer.valueOf(args[1]);
      length = Integer.valueOf(args[2]);
      useThreadPool = Boolean.valueOf(args[3]);
    }

    if (args.length == 5) {
      sendBytes = Boolean.valueOf(args[4]);
    }

    EventLoopGroup group = new NioEventLoopGroup();
    try {
      Bootstrap b = new Bootstrap();
      b.group(group).channel(NioSocketChannel.class).handler(new HelloClientInitializer(sendBytes));

      Channel ch = b.connect(host, port).sync().channel();

      logger.warn("channel's high water mark is {} ", ch.config().getWriteBufferHighWaterMark());
      logger.warn("channel's low water mark is {} ", ch.config().getWriteBufferLowWaterMark());
      //            ch.config().setWriteBufferLowWaterMark(512);
      //            ch.config().setWriteBufferHighWaterMark(1024);
      logger.warn("channel's high water mark is {} ", ch.config().getWriteBufferHighWaterMark());
      logger.warn("channel's low water mark is {} ", ch.config().getWriteBufferLowWaterMark());
      byte[] data = new byte[length];
      for (int i = 0; i < length; i++) {
        data[i] = 'a';
      }

      PyWriteRequest request = productPyWriteRequest(length);
      if (!useThreadPool) {
        logger.warn("NOT use thread pool to send requests");
        for (; ; ) {
          ioDepth.acquire();
          if (sendBytes) {
            ch.writeAndFlush(data);
          } else {
            ch.writeAndFlush(request);
          }
        }
      } else {
        logger.warn("USE thread pool to send requests");
        final boolean sendBytesFlag = sendBytes;
        for (; ; ) {
          try {
            ioDepth.acquire();
          } catch (InterruptedException e) {
            logger.error("caught an exception", e);
          }
          netty_client_sender.execute(new Runnable() {
            @Override
            public void run() {
              if (sendBytesFlag) {
                ch.writeAndFlush(data);
              } else {
                ch.writeAndFlush(request);
              }
            }
          });
        }
      }
    } finally {
      group.shutdownGracefully();
    }
  }

  private static PyWriteRequest productPyWriteRequest(int writeRequestLength) {
    PyWriteRequest pyWriteRequest = SimpleWriteRequestBuilder
        .generateSimpleWriteRequest(writeRequestLength, 1);
    return pyWriteRequest;
  }
}