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

import java.util.HashMap;
import java.util.Map;
import org.apache.thrift.TException;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.dih.client.DihClientRequestResponseHelper;
import py.instance.Instance;
import py.instance.PortType;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.distributedinstancehub.service.DistributedInstanceHub;
import py.thrift.distributedinstancehub.service.GetInstanceRequest;
import py.thrift.distributedinstancehub.service.GetInstanceResponse;
import py.thrift.distributedinstancehub.service.InstanceThrift;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;

/**
 * this class is used for dih.
 *
 */
public class SetDataNodeConfigurationCmd extends AbstractCmd {

  public SetDataNodeConfigurationCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    if (args == null || args.length != 2) {
      usage();
      return;
    }

    GetInstanceResponse response = null;
    try {
      GetInstanceRequest request = new GetInstanceRequest();
      response = ((DistributedInstanceHub.Iface) debugConfigurator).getInstances(request);
    } catch (TException e) {
      out.print("can not get service");
      e.printStackTrace();
    }

    SetConfigurationsRequest setRequest = new SetConfigurationsRequest();
    GetConfigurationsRequest viewRequest = new GetConfigurationsRequest();
    GetConfigurationsResponse viewResponse = null;

    String key = args[0];
    String value = args[1];

    GenericThriftClientFactory<DataNodeService.Iface> dataNodeServiceFactory =
        GenericThriftClientFactory
            .create(DataNodeService.Iface.class);
    for (InstanceThrift instanceThrift : response.getInstanceList()) {
      if (instanceThrift.getName().equals("DataNode")) {
        Instance dataNodeInstance = null;
        try {
          dataNodeInstance = DihClientRequestResponseHelper.buildInstanceFrom(instanceThrift);
        } catch (Exception e) {
          System.out.println(e);
        }
        String ip = dataNodeInstance.getEndPointByServiceName(PortType.CONTROL).getHostName();
        int port = dataNodeInstance.getEndPointByServiceName(PortType.CONTROL).getPort();
        String hostAndPort = ip + ":" + port;
        out.print("\nget a datanode " + hostAndPort);

        try {
          EndPoint endPointOfDataNode = new EndPoint(hostAndPort);
          DataNodeService.Iface dataNodeClient = dataNodeServiceFactory
              .generateSyncClient(endPointOfDataNode, 5000, 10000);

          // check the key if exist
          try {
            viewRequest.setRequestId(0xffff);
            viewResponse = dataNodeClient.getConfigurations(viewRequest);
          } catch (Exception ex) {
            ex.printStackTrace();
            return;
          }

          boolean foundKey = false;
          Map<String, String> keyAndValue = new HashMap<String, String>();
          Map<String, String> configurations = viewResponse.getResults();
          for (String mapKey : configurations.keySet()) {
            if (mapKey.equalsIgnoreCase(key)) {
              keyAndValue.put(mapKey, value);
              foundKey = true;
              break;
            }
          }

          if (!foundKey) {
            out.print("key " + key + " does not exist in service configuration");
            return;
          }

          try {
            setRequest.setConfigurations(keyAndValue);
            setRequest.setRequestId(0xffffL);
            dataNodeClient.setConfigurations(setRequest);
          } catch (Exception ex) {
            ex.printStackTrace();
            return;
          }
          out.print(
              "Set " + key + " with value " + value + " at " + hostAndPort + " is successful");
        } catch (Exception ex) {
          out.print("can not connect the datanode " + ex.toString());
        }
      }
    }
  }

  @Override
  public void usage() {
    StringBuffer sb = new StringBuffer();
    sb.append("setConfigOfDataNode key value\n");
    out.print(sb.toString());
  }

}