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
