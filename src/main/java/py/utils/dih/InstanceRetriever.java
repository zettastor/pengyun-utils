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
