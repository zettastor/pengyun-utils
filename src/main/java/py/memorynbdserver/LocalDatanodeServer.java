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

import static py.common.Utils.millsecondToString;

import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.memorynbdserver.mocktools.MockCoordinatorBuilder;
import py.memorynbdserver.mocktools.MockDatanodeService;
import py.netty.datanode.AsyncDataNode;
import py.netty.exception.InvalidProtocolException;
import py.netty.message.ProtocolBufProtocolFactory;
import py.netty.server.GenericAsyncServer;
import py.netty.server.GenericAsyncServerBuilder;

/**
 * xx.
 */
public class LocalDatanodeServer {

  private static final Logger logger = LoggerFactory.getLogger(LocalDatanodeServer.class);

  /**
   * xx.
   */
  public void start(int segmentCount, int port) {

    ByteBuffer byteBuffer = ByteBuffer
        .allocate(segmentCount * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE);
    // start netty server, process write or read request
    MockDatanodeService mockDatanodeService = new MockDatanodeService(byteBuffer);
    GenericAsyncServerBuilder serverFactory = null;
    try {
      serverFactory = new GenericAsyncServerBuilder(mockDatanodeService,
          ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
          GenericAsyncServerBuilder.defaultConfiguration());
    } catch (InvalidProtocolException e) {
      logger.error("", e);
    }
    serverFactory.setMaxIoPendingRequests(1000);
    serverFactory.setAllocator(PooledByteBufAllocator.DEFAULT);
    String ipAddress = null;

    EndPoint ioEndpoint = new EndPoint(ipAddress, port);

    logger.warn("start listen port: {} for tcp server", ioEndpoint);
    try {
      GenericAsyncServer genericAsyncServer = serverFactory.build(ioEndpoint);

      while (true) {
        Long currentTimeMs = System.currentTimeMillis();
        String currentTimeStr = millsecondToString(currentTimeMs);
        System.out
            .println("I am a local datanode server, and still alive now:[" + currentTimeStr + "]");
        Thread.sleep(5000);
      }
    } catch (InterruptedException e) {
      logger.error("", e);
    }
  }
}
