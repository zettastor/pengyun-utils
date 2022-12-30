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
import org.apache.thrift.TException;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.exception.GenericThriftClientFactoryException;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.InvalidateCacheRequest;
import py.thrift.datanode.service.InvalidateCacheResponse;
import py.thrift.datanode.service.NotSupportedExceptionThrift;

/**
 * xx.
 */
public class InvalidateL1CacheUtil {

  private static final String IPADDRESS_PATTERN =
      "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
  private static final String L1 = "l1";
  private static final String L2 = "l2";
  public static int dataNodePort = 10011;

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      usage();
    }
    GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = 
        GenericThriftClientFactory
        .create(DataNodeService.Iface.class).withMaxChannelPendingSizeMb(200);
    List<String> ips = getIps(args[0]);

    String l1Orl2 = args[1];
    int cacheLevel = 0;
    if (l1Orl2.equalsIgnoreCase(L1)) {
      cacheLevel = 1;
    } else if (l1Orl2.equalsIgnoreCase(L2)) {
      cacheLevel = 2;
    } else {
      usage();
    }

    for (String ip : ips) {
      boolean done = false;
      int tryTimes = 10;
      while (!done && tryTimes > 0) {
        EndPoint endPoint = EndPointParser.parseLocalEndPoint(dataNodePort, ip);
        try {
          DataNodeService.Iface client = dataNodeSyncClientFactory
              .generateSyncClient(endPoint, 15000, 10000);
          InvalidateCacheRequest request = new InvalidateCacheRequest();
          request.setMagicNumber(0L);
          request.setCacheLevel(cacheLevel);
          InvalidateCacheResponse response = client.invalidateCache(request);
          done = response.done;
          tryTimes--;
          if (!done) {
            System.out.println("for the ip " + ip + " , not done yet, let's try again");
          } else {
            System.out.println("for the ip " + ip + " , operation success!");
          }
        } catch (GenericThriftClientFactoryException e) {
          e.printStackTrace();
          System.out.println("fail to connect to datanode, wait 5 seconds to try again");
          try {
            Thread.sleep(5000);
          } catch (Exception ex) {
            ex.printStackTrace();
          }
        } catch (NotSupportedExceptionThrift e) {
          e.printStackTrace();
        } catch (TException e) {
          e.printStackTrace();
        }
      }
    }

  }

  private static List<String> getIps(String ips) {
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
    System.out.println("usage: \n\tjava -jar InvalidateL1CacheUtil.jar ips l1");
    System.out.println("example: \n\tjava -jar InvalidateL1CacheUtil.jar 10.0.1.140:10.0.1.143 l2");
    System.out.println(
        "\tOR:\n\tjava -jar InvalidateL1CacheUtil." 
            + "jar 10.0.1.140,10.0.1.141,10.0.1.142,10.0.1.143 l1");
    System.exit(-1);
  }

}
