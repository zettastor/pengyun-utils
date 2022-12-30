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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.thrift.protocol.TProtocolFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.thrift.datanode.service.DataNodeService;

public class MockDataNodeSyncClientFactory extends
    GenericThriftClientFactory<DataNodeService.Iface> {

  public MockDataNodeSyncClientFactory(Class<DataNodeService.Iface> serviceInterface,
      TProtocolFactory factory,
      int numWorkerThreadCount, int minResponseThreadCount, int maxResponseThreadCount,
      boolean closeSafe) {
    super(serviceInterface, factory, numWorkerThreadCount, minResponseThreadCount,
        maxResponseThreadCount, closeSafe);
  }

  @Override
  public DataNodeService.Iface generateSyncClient(final EndPoint endPoint, long socketTimeoutMs,
      int connectionTimeoutMs) {
    return (DataNodeService.Iface) Proxy
        .newProxyInstance(DataNodeService.Iface.class.getClassLoader(),
            new Class[]{DataNodeService.Iface.class}, new Handler());
  }

  static class Handler implements InvocationHandler {

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return null;
    }
  }

}
