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

package py.utils.dih;

import java.util.Set;
import org.apache.thrift.TException;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;

/**
 * retrieve all instance from local DIH.
 */
public class InstanceRetriever {

  private static final long requestTimeout = 20000;

  private DihClientFactory dihClientFactory;

  private EndPoint dihEndPoint;

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  public EndPoint getDihEndPoint() {
    return dihEndPoint;
  }

  public void setDihEndPoint(EndPoint dihEndPoint) {
    this.dihEndPoint = dihEndPoint;
  }

  /**
   * xx.
   */
  public Set<Instance> retrieve() {
    try {
      DihServiceBlockingClientWrapper client = dihClientFactory.build(dihEndPoint, requestTimeout);
      return client.getInstanceAll();
    } catch (GenericThriftClientFactoryException e) {
      System.out.println("Failed to build client");
    } catch (TException e) {
      System.out.println("Failed to get all instances, no available DIH");
    }
    return null;
  }
}
