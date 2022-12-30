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

package py.utils.test;

import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;

/**
 * xx.
 */
public class ShutdownTest {

  /**
   * xx.
   */
  public static void main(String[] args) {
    DihClientFactory dihClientFactory = null;
    try {
      dihClientFactory = new DihClientFactory(1);
      EndPoint endPoint = new EndPoint("10.0.1.112", 10000);
      DihServiceBlockingClientWrapper client = dihClientFactory.build(endPoint, 20000);

      client.getDelegate().shutdown();
    } catch (Exception e) {
      System.out.println("exception occur");
    } finally {
      if (dihClientFactory != null) {
        dihClientFactory.close();
      }
    }
  }

}
