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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * xx.
 */
public class ConsoleLogger {

  /**
   * xx.
   */
  public static void log(String message) {
    SimpleDateFormat simpleDateFormat = getSimpleDateFormat();
    System.out.println("\n Thread=" + Thread.currentThread().getId() + ", time="
        + simpleDateFormat.format(new Date()) + ", message:" + message);
  }

  public static String getFormatType() {
    return "yyyy-MM-dd hh:mm:ss:SSS";
  }

  public static SimpleDateFormat getSimpleDateFormat() {
    return new SimpleDateFormat(getFormatType());
  }

  public static long stringToLongTime(String time) throws ParseException {
    SimpleDateFormat formatter = new SimpleDateFormat(getFormatType());
    return formatter.parse(time).getTime();
  }

  public static String longTimeToString(long time) {
    return getSimpleDateFormat().format(new Date(time));
  }
}
