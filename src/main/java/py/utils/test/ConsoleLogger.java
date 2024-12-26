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
