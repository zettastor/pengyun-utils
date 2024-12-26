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