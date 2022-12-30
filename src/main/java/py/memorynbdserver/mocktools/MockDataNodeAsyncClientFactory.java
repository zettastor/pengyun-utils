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
import py.exception.GenericThriftClientFactoryException;
import py.thrift.datanode.service.DataNodeService;

public class MockDataNodeAsyncClientFactory extends
    GenericThriftClientFactory<DataNodeService.AsyncIface> {

  protected MockDataNodeAsyncClientFactory(Class<DataNodeService.AsyncIface> serviceInterface,
      TProtocolFactory factory, int numWorkerThreadCount, int minResponseThreadCount,
      int maxResponseThreadCount, boolean closeSafe) {
    super(serviceInterface, factory, numWorkerThreadCount, minResponseThreadCount,
        maxResponseThreadCount, closeSafe);
  }

  @Override
  public DataNodeService.AsyncIface generateAsyncClient(final EndPoint endPoint,
      long socketTimeoutMs, int connectionTimeoutMs)
      throws GenericThriftClientFactoryException {
    return (DataNodeService.AsyncIface) Proxy
        .newProxyInstance(DataNodeService.AsyncIface.class.getClassLoader(),
            new Class[]{DataNodeService.AsyncIface.class}, new Handler());
  }

  static class Handler implements InvocationHandler {

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      return null;
    }
  }
}
