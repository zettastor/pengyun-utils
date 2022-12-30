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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class HelloServer {

  private static final Logger logger = LoggerFactory.getLogger(HelloServer.class);

  /**
   * xx.
   */
  public static void main(String[] args) throws InterruptedException {

    String host = "127.0.0.1";
    int port = 7878;
    int packageSize = 8192;
    if (args.length == 3) {
      host = args[0];
      port = Integer.valueOf(args[1]);
      packageSize = Integer.valueOf(args[2]);
    }

    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup);
      b.channel(NioServerSocketChannel.class);
      b.childHandler(new HelloServerInitializer(packageSize));

      ChannelFuture f = b.bind(host, port).sync();

      while (true) {
        logger.warn(">>>>server:{}@{} still alive now<<<<", host, port);
        Thread.sleep(5000);
      }
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }
}