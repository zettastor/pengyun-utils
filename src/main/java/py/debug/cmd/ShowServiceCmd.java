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

package py.debug.cmd;

import java.util.HashSet;
import java.util.Set;
import org.apache.thrift.TException;
import py.dih.client.DihClientRequestResponseHelper;
import py.instance.Instance;
import py.instance.PortType;
import py.thrift.distributedinstancehub.service.DistributedInstanceHub;
import py.thrift.distributedinstancehub.service.GetInstanceRequest;
import py.thrift.distributedinstancehub.service.GetInstanceResponse;
import py.thrift.distributedinstancehub.service.InstanceThrift;
import py.thrift.share.DebugConfigurator;

/**
 * this class is used for dih.
 *
 */
public class ShowServiceCmd extends AbstractCmd {

  public ShowServiceCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    Set<Instance> instances = new HashSet<Instance>();
    GetInstanceResponse response = null;
    try {
      GetInstanceRequest request = new GetInstanceRequest();
      response = ((DistributedInstanceHub.Iface) debugConfigurator).getInstances(request);
    } catch (TException e) {
      out.print("can not get service");
      e.printStackTrace();
    }

    for (InstanceThrift instanceThrift : response.getInstanceList()) {
      try {
        instances.add(DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift));
      } catch (Exception e) {
        System.out.println(e);
      }
    }

    StringBuffer sb = new StringBuffer();
    int length = 25;
    sb.append(
        "|Id                       " + "|Name                     " + "|Host                     "
            + "|Port                     " + "|Status                   |");

    sb.append("\n");
    for (Instance instance : instances) {
      int idWordCount = String.valueOf(instance.getId().getId()).length();
      sb.append("|" + instance.getId().getId());
      for (int i = 0; i < length - idWordCount; i++) {
        sb.append(" ");
      }

      int idNameCount = instance.getName().length();
      sb.append("|" + instance.getName());
      for (int i = 0; i < length - idNameCount; i++) {
        sb.append(" ");
      }

      // this will show the first endpoint of every instance, because it is the main service
      // by default in a instance.
      int idHostCount = instance.getEndPointByServiceName(PortType.CONTROL).getHostName().length();
      sb.append("|" + instance.getEndPointByServiceName(PortType.CONTROL).getHostName());
      for (int i = 0; i < length - idHostCount; i++) {
        sb.append(" ");
      }

      int idPortCount = String
          .valueOf(instance.getEndPointByServiceName(PortType.CONTROL).getPort())
          .length();
      sb.append("|" + instance.getEndPointByServiceName(PortType.CONTROL).getPort());
      for (int i = 0; i < length - idPortCount; i++) {
        sb.append(" ");
      }

      int idStatusCount = instance.getStatus().name().length();
      sb.append("|" + instance.getStatus().name());
      for (int i = 0; i < length - idStatusCount; i++) {
        sb.append(" ");
      }

      sb.append("|\n");
    }
    out.print(sb.toString());
  }

  @Override
  public void usage() {

  }
}
