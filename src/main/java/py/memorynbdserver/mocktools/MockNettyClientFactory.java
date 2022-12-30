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

package py.memorynbdserver.mocktools;

import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.EndPoint;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.ProtocolFactory;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.exception.InvalidProtocolException;
import py.netty.message.ProtocolBufProtocolFactory;
import py.nettysetup.SimpleWriteCallback;

public class MockNettyClientFactory extends GenericAsyncClientFactory<AsyncDataNode.AsyncIface> {

  private static final Logger logger = LoggerFactory.getLogger(MockNettyClientFactory.class);
  GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private Map<EndPoint, ByteBuffer> mapEndPoint2ByteBuffer;
  private EndPoint serverEndPoint;
  private AsyncDataNode.AsyncIface client;

  /**
   * xx.
   */
  public MockNettyClientFactory(Class<AsyncDataNode.AsyncIface> serviceInterface,
      ProtocolFactory protocolFactory,
      TransferenceConfiguration cfg, Map<EndPoint, ByteBuffer> mapEndPoint2ByteBuffer,
      ByteBufAllocator allocator,
      EndPoint serverEndPoint) {
    super(serviceInterface, protocolFactory, cfg);

    this.mapEndPoint2ByteBuffer = mapEndPoint2ByteBuffer;
    this.serverEndPoint = serverEndPoint;

    if (this.serverEndPoint != null) {
      logger.warn("going to construct netty client factory");
      try {
        clientFactory = new GenericAsyncClientFactory<>(AsyncDataNode.AsyncIface.class,
            ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class), cfg);
        clientFactory.setAllocator(allocator);
        clientFactory.init();
        for (int i = 0; i < 10; i++) {
          this.client = clientFactory.generate(this.serverEndPoint);
          this.client.ping(new SimpleWriteCallback<>());
          try {
            Thread.sleep(300);
          } catch (InterruptedException e) {
            logger.error("caught an special exception", e);
          }
        }
      } catch (InvalidProtocolException e) {
        logger.error("caught an exception", e);
      } catch (Throwable e) {
        logger.error("catch exception", e);
        throw e;
      }
    }
  }

  @Override
  public AsyncDataNode.AsyncIface generate(EndPoint endPoint) {
    ByteBuffer byteBuffer = this.mapEndPoint2ByteBuffer.get(endPoint);

    boolean isPrimary;
    if (serverEndPoint == null) {
      isPrimary = MockCoordinatorBuilder.isPrimary(endPoint);
    } else {
      isPrimary = endPoint.equals(serverEndPoint);
    }

    AsyncDataNode.AsyncIface client;

    if (this.serverEndPoint != null && isPrimary) {
      client = clientFactory.generate(this.serverEndPoint);
    } else {
      client = new MockDatanodeNettyIface(byteBuffer, isPrimary);
    }

    return client;
  }
}
