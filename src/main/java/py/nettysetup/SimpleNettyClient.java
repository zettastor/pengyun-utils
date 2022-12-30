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
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.informationcenter.Utils;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.client.TransferenceClientOption;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.PyWriteRequest;
import py.netty.exception.InvalidProtocolException;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.netty.message.ProtocolBufProtocolFactory;
import py.proto.Broadcastlog;

public class SimpleNettyClient {

  private static final Logger logger = LoggerFactory.getLogger(SimpleNettyClient.class);
  private static GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private static LinkedBlockingDeque<PyWriteRequest> writeTaskQueue;
  private static LinkedBlockingDeque<Broadcastlog.PbReadRequest> readTaskQueue;

  private static int ioRequestLength;
  private static int eachRequestCarryNumberOfUnits;
  private static boolean isWrite;
  private static int queueLength;

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (args.length != 7) {
      usage();
      System.exit(-1);
    }

    final String hostname = args[0];
    final int port = Integer.valueOf(args[1]);

    ioRequestLength = (int) Utils.getByteSize(args[2]);
    eachRequestCarryNumberOfUnits = Integer.valueOf(args[3]);
    final int ioDepth = Integer.valueOf(args[4]);

    isWrite = Boolean.valueOf(args[5]);

    queueLength = Integer.valueOf(args[6]);

    if (isWrite) {
      writeTaskQueue = new LinkedBlockingDeque<>(queueLength);
    } else {
      readTaskQueue = new LinkedBlockingDeque<>(queueLength);
    }

    EndPoint connectEndPoint = new EndPoint(hostname, port);

    CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();

    TransferenceConfiguration transferenceConfiguration = TransferenceConfiguration
        .defaultConfiguration();
    transferenceConfiguration
        .option(TransferenceClientOption.CONNECTION_COUNT_PER_ENDPOINT, 1);
    transferenceConfiguration
        .option(TransferenceClientOption.IO_TIMEOUT_MS, cfg.getNettyRequestTimeoutMs());
    transferenceConfiguration
        .option(TransferenceClientOption.IO_CONNECTION_TIMEOUT_MS, cfg.getNettyConnectTimeoutMs());

    Semaphore semaphore = new Semaphore(ioDepth);
    logger.warn("going to construct netty client factory");
    try {
      clientFactory = new GenericAsyncClientFactory<>(AsyncDataNode.AsyncIface.class,
          ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
          transferenceConfiguration);


      ByteBufAllocator byteBufAllocator = PooledByteBufAllocatorWrapper.INSTANCE;
      clientFactory.setAllocator(byteBufAllocator);
      clientFactory.init();

    } catch (InvalidProtocolException e) {
      logger.error("caught an exception", e);
    }

    logger.error("version 1");

    Thread producer = new Thread(() -> producePyWriteRequest());

    producer.start();

    PyWriteRequest pyWriteRequestToDiscard = null;
    Broadcastlog.PbReadRequest readRequest = null;
    while (true) {

      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        logger.error("caught an exception", e);
      }

      if (isWrite) {
        try {
          pyWriteRequestToDiscard = writeTaskQueue.take();
        } catch (InterruptedException e) {
          logger.error("caught an exception", e);
        }

        Long requestId = pyWriteRequestToDiscard.getMetadata().getRequestId();
        SimpleWriteCallback simpleWriteCallback = new SimpleWriteCallback(requestId, semaphore);

        Validate.notNull(pyWriteRequestToDiscard);
        clientFactory.generate(connectEndPoint).write(pyWriteRequestToDiscard, simpleWriteCallback);
      } else {
        try {
          readRequest = readTaskQueue.take();
        } catch (InterruptedException e) {
          logger.error("caught an exception", e);
        }

        Long requestId = readRequest.getRequestId();
        SimpleReadCallback simpleReadCallback = new SimpleReadCallback(requestId, semaphore);

        Validate.notNull(readRequest);
        clientFactory.generate(connectEndPoint).read(readRequest, simpleReadCallback);
      }
    }
  }

  private static void producePyWriteRequest() {
    while (true) {
      if (isWrite) {
        PyWriteRequest pyWriteRequest = SimpleWriteRequestBuilder
            .generateDiscardSimpleWriteRequest(ioRequestLength, eachRequestCarryNumberOfUnits);
        writeTaskQueue.offer(pyWriteRequest);
      } else {
        Broadcastlog.PbReadRequest pbReadRequest = SimpleReadRequestBuilder
            .generateSimpleReadRequest(ioRequestLength, eachRequestCarryNumberOfUnits);
        readTaskQueue.offer(pbReadRequest);
      }
    }
  }

  private static void usage() {
    System.out.println("please give connect hostname and port, like \"10.0.1.19\", \"8888\"\n"
        + "and write unit length like 8k\n" + "and each request carry number of units, like 1,2,3\n"
        + "and io depth which is must be positive\n"
        + "and should target IO type you want, such as[true:write, false:read]\n"
        + "and produce queue length\n");
  }
}
