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

/**
 * xx.
 */
public class ConstantHolder {

  public static String HELLO = "hello";

  static {
    HELLO = "123";
    HELLO = "456";
  }

  static {
    HELLO = "000000000000000000000000000000000000";
  }

  public String aa = "123";
  private String ss = "123";

  public ConstantHolder() {
    ss = "456";
    aa = "456";
  }

  @Override
  public String toString() {
    return "ConstantHolder [ss=" + ss + ", aa=" + aa + "]";
  }
}
