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

package py.nettysetup;

import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.netty.datanode.AsyncDataNode;
import py.netty.exception.InvalidProtocolException;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.netty.message.ProtocolBufProtocolFactory;
import py.netty.server.GenericAsyncServer;
import py.netty.server.GenericAsyncServerBuilder;

public class SimpleNettyServer {

  private static final Logger logger = LoggerFactory.getLogger(SimpleNettyServer.class);

  public static void main(String[] args) {

    if (args.length != 2) {
      usage();
      System.exit(-1);
    }

    String hostname = args[0];
    int port = Integer.valueOf(args[1]);
    try {
      ByteBufAllocator byteBufAllocator = PooledByteBufAllocatorWrapper.INSTANCE;
      SimpleDatanodeIoServiceImpl processor = new SimpleDatanodeIoServiceImpl(byteBufAllocator);
      GenericAsyncServerBuilder serverFactory = new GenericAsyncServerBuilder(processor,
          ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
          GenericAsyncServerBuilder.defaultConfiguration());
      serverFactory.setMaxIoPendingRequests(1000);
      serverFactory.setAllocator(byteBufAllocator);
      EndPoint ioEndpoint = new EndPoint(hostname, port);

      logger.warn("start listen port: {} for tcp server", ioEndpoint);

      GenericAsyncServer genericAsyncServer = serverFactory.build(ioEndpoint);

      // gonna run until someone interrupt
      while (true) {
        Thread.sleep(50000);
        logger.warn(">>>>{} server still listening<<<<", ioEndpoint);
      }
    } catch (InterruptedException | InvalidProtocolException e) {
      logger.error("caught an exception", e);
      throw new RuntimeException();
    }

  }

  private static void usage() {
    System.out.println("please give hostname and port to listen, like \"10.0.1.19\", \"8888\"");
  }
}
