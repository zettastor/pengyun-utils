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
