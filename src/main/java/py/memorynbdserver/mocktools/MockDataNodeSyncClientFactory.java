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
