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

import java.net.InetAddress;
import java.util.Set;
import py.common.struct.EndPointParser;
import py.dih.client.DihClientFactory;
import py.instance.Instance;
import py.instance.PortType;

/**
 * xx.
 */
public class Launcher {

  private static final int DIHPORT = 10000;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    InstanceRetriever retriever = new InstanceRetriever();
    if (args.length != 0) {
      retriever.setDihEndPoint(
          EndPointParser.parseLocalEndPoint(args[0], InetAddress.getLocalHost().getHostAddress()));
    } else {
      retriever.setDihEndPoint(
          EndPointParser.parseLocalEndPoint(DIHPORT, InetAddress.getLocalHost().getHostAddress()));
    }
    retriever.setDihClientFactory(dihClientFactory);
    Set<Instance> instances = retriever.retrieve();
    if (instances == null || instances.size() == 0) {
      System.out.println("No instances");
      System.exit(0);
    }

    String tableHeader = String
        .format("\n|%-25s|%-6s|%-20s|%-15s|%-37s|%-8s|%-48s|", "Id", "Group", "Name", "Host",
            "Port", "Status", "Location");
    String subHeader = String
        .format("|%-25s|%-6s|%-20s|%-15s|%-8s|%-10s|%-8s|%-8s|%-8s|", " ", " ", " ", " ",
            "Control", "HeartBeat", "IO", "Monitor", " ");

    System.out.println(tableHeader);
    System.out.println(subHeader);
    for (int i = 0; i < tableHeader.length(); i++) {
      System.out.print("-");
    }

    System.out.println();
    for (Instance instance : instances) {
      final String id = String.valueOf(instance.getId().getId());
      final String group =
          (instance.getGroup() == null) ? "" : String.valueOf(instance.getGroup().getGroupId());
      final String name = instance.getName();
      final String host = instance.getEndPoint().getHostName();

      final String controlPort = String.valueOf(instance.getEndPoint().getPort());

      String monitorPort = "";
      if (instance.getEndPointByServiceName(PortType.MONITOR) != null) {
        monitorPort = String.valueOf(instance.getEndPointByServiceName(PortType.MONITOR).getPort());
      }

      String ioPort = "";
      if (instance.getEndPointByServiceName(PortType.IO) != null) {
        ioPort = String.valueOf(instance.getEndPointByServiceName(PortType.IO).getPort());
      }

      String heartBeatPort = "";
      if (instance.getEndPointByServiceName(PortType.HEARTBEAT) != null) {
        ioPort = String.valueOf(instance.getEndPointByServiceName(PortType.HEARTBEAT).getPort());
      }

      String status = instance.getStatus().name();

      String location = (instance.getLocation() == null) ? "" : instance.getLocation().toString();

      String tableRow = String
          .format("|%-25s|%-6s|%-20s|%-15s|%-8s|%-10s|%-8s|%-8s|%-8s|%-48s|\n", id, group,
              name, host, controlPort, heartBeatPort, ioPort, monitorPort, status, location);
      System.out.print(tableRow);
    }
    System.out.println();

    dihClientFactory.close();
  }

}
