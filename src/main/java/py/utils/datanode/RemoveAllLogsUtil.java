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

import java.util.List;
import org.apache.thrift.TException;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.exception.GenericThriftClientFactoryException;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.NotSupportedExceptionThrift;
import py.thrift.datanode.service.ReleaseAllLogsRequest;

/**
 * xx.
 */
public class RemoveAllLogsUtil {

  public static int dataNodePort = 10011;

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (args.length != 1) {
      usage();
    }
    GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = 
        GenericThriftClientFactory
        .create(DataNodeService.Iface.class).withMaxChannelPendingSizeMb(200);
    List<String> ips = IpUtils.getIps(args[0]);

    long magicNumberForReleaseLogs = 0x042028;
    boolean result = true;
    for (String ip : ips) {
      EndPoint endPoint = EndPointParser.parseLocalEndPoint(dataNodePort, ip);
      try {
        DataNodeService.Iface client = dataNodeSyncClientFactory
            .generateSyncClient(endPoint, 15000, 10000);
        ReleaseAllLogsRequest request = new ReleaseAllLogsRequest(magicNumberForReleaseLogs);
        client.releaseAllLogs(request);
        System.out.println("for the ip " + ip + " , operation success!");
      } catch (GenericThriftClientFactoryException e) {
        e.printStackTrace();
        System.out.println("fail to connect to datanode, wait 5 seconds to try again");
        try {
          Thread.sleep(5000);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
        result = false;
      } catch (NotSupportedExceptionThrift e) {
        e.printStackTrace();
        result = false;
      } catch (TException e) {
        System.out.println("Check magic number?" + e);
        result = false;
      }
    }
    if (!result) {
      System.out.println("failed to clean log for ips : " + ips);
      System.exit(-1);
    }
  }

  /**
   * xx.
   */
  public static void usage() {
    System.out.println("example: \n\tjava -jar ReleaseAllLogs.jar 10.0.1.140:10.0.1.143");
    System.out.println(
        "\tOR:\n\tjava -jar ReleaseAllLogs.jar 10.0.1.140,10.0.1.141,10.0.1.142,10.0.1.143");
    System.exit(-1);
  }
}
