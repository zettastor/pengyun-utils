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
import static py.informationcenter.Utils.getByteSize;

import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.memorynbdserver.mocktools.MockCoordinatorBuilder;
import py.memorynbdserver.mocktools.MockDatanodeService;
import py.netty.datanode.AsyncDataNode;
import py.netty.exception.InvalidProtocolException;
import py.netty.message.ProtocolBufProtocolFactory;
import py.netty.server.GenericAsyncServerBuilder;

public class MemoryDatanodeLauncher {

  public static final Logger logger = LoggerFactory.getLogger(MemoryDatanodeLauncher.class);

  /**
   * xx.
   */
  public static void main(String[] args) throws InvalidProtocolException {
    if (args.length != 3) {
      usage();
      System.exit(-1);
    }

    int segmentCount = Integer.valueOf(args[0]);
    if (segmentCount <= 0) {
      System.out.println("given segment count should be a positive number");
      System.exit(-1);
    }

    String ipAddress = null;
    try {
      ipAddress = args[1];
      Validate.isTrue(Utils.isIpAddress(ipAddress));
    } catch (Exception e) {
      System.out.println("given ip address to listen should be a valid format");
      usage();
      System.exit(-1);
    }

    int port = (int) getByteSize(args[2]);
    if (port <= 0 || port > 65535) {
      System.out.println("given port should between 0 to 65535");
      System.exit(-1);
    }

    ByteBuffer byteBuffer = ByteBuffer
        .allocate(segmentCount * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE);
    // start netty server, process write or read request
    MockDatanodeService mockDatanodeService = new MockDatanodeService(byteBuffer);
    GenericAsyncServerBuilder serverFactory = new GenericAsyncServerBuilder(mockDatanodeService,
        ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
        GenericAsyncServerBuilder.defaultConfiguration());
    serverFactory.setMaxIoPendingRequests(Integer.MAX_VALUE - 1);
    serverFactory.setAllocator(PooledByteBufAllocator.DEFAULT);
    EndPoint ioEndpoint = new EndPoint(ipAddress, port);

    logger.warn("start listen port: {} for tcp server", ioEndpoint);
    try {
      serverFactory.build(ioEndpoint);

      while (true) {
        Long currentTimeMs = System.currentTimeMillis();
        String currentTimeStr = millsecondToString(currentTimeMs);
        System.out
            .println("I am a memory datanode server, and still alive now:[" + currentTimeStr + "]");
        Thread.sleep(5000);
      }
    } catch (InterruptedException ignored) {
      logger.error("", ignored);
    }
  }

  private static void usage() {
    String usageInfo = "usage: java -jar MemoryDatanodeLauncher.jar segmentCount ipAddress port \n"
        + "param:segmentCount [1, 512](means use memory size:[1MB, 512MB])\n"
        + "param:ipAddress (like \"127.0.0.1\")\n" + "param:port (0-65535] will be fine\n";
    System.out.println(usageInfo);
  }

}
