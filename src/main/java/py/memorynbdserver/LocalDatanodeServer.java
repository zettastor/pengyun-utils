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
