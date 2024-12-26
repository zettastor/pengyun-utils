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
