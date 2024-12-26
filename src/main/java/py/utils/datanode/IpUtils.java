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

package py.utils.datanode;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * xx.
 */
public class IpUtils {

  private static final String IPADDRESS_PATTERN =
      "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

  /**
   * xx.
   */
  public static List<String> getIps(String ips) {
    Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
    List<String> ipParts = new ArrayList<String>();
    if (ips.contains(",")) {
      String[] ipPartsArray = ips.split(",");
      for (String ipPart : ipPartsArray) {
        ipParts.add(ipPart);
      }
    } else {
      ipParts.add(ips);
    }
    List<String> ipList = new ArrayList<String>();
    for (String ipPart : ipParts) {
      if (!ipPart.contains(":")) {
        Matcher matcher = pattern.matcher(ipPart);
        if (!matcher.matches()) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        } else {
          ipList.add(ipPart);
          continue;
        }
      }
      String[] ip = ipPart.split(":");
      if (ip.length != 2) {
        System.out.println("Are you kidding me? Correct ip address OK ?!");
        usage();
      }
      for (String eachIp : ip) {
        Matcher matcher = pattern.matcher(eachIp);
        if (!matcher.matches()) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        }
      }
      String[] ipUnit1 = ip[0].split("\\.");
      String[] ipUnit2 = ip[1].split("\\.");
      for (int i = 0; i < 3; i++) {
        if (!ipUnit1[i].endsWith(ipUnit2[i])) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        }
      }
      int start = Integer.parseInt(ipUnit1[3]);
      int end = Integer.parseInt(ipUnit2[3]);
      if (start > end) {
        int tmp = start;
        start = end;
        end = tmp;
      }
      for (int i = start; i <= end; i++) {
        ipList.add(ipUnit1[0] + "." + ipUnit1[1] + "." + ipUnit1[2] + "." + String.valueOf(i));
      }
    }
    return ipList;
  }

  /**
   * xx.
   */
  public static void usage() {
    System.out.println("For instance: 10.0.1.140:10.0.1.143");
    System.out.println("\tOR:\n\t10.0.1.140,10.0.1.141,10.0.1.142,10.0.1.143");
    System.exit(-1);
  }
}
