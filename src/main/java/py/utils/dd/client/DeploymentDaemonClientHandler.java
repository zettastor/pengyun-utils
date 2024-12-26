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

package py.utils.dd.client;

import org.apache.log4j.Logger;
import py.DeploymentDaemonClientFactory;
import py.common.RequestIdBuilder;
import py.thrift.deploymentdaemon.ActivateRequest;
import py.thrift.deploymentdaemon.DeactivateRequest;
import py.thrift.deploymentdaemon.DeploymentDaemon;

/**
 * Deployment daemon client.
 */
public class DeploymentDaemonClientHandler {

  private static final Logger logger = Logger.getLogger(DeploymentDaemonClientHandler.class);
  private DeploymentDaemonClientFactory deploymentDaemonClientFactory;

  public void put() {

  }

  public void setDeploymentDaemonClientFactory(
      DeploymentDaemonClientFactory deploymentDaemonClientFactory) {
    this.deploymentDaemonClientFactory = deploymentDaemonClientFactory;
  }

  /**
   * xx.
   */
  public void activate() {
    try {
      final DeploymentDaemon.Iface client = 
          deploymentDaemonClientFactory.build("10.0.1.112", 10002)
          .getClient();

      ActivateRequest request = new ActivateRequest();
      request.setRequestId(RequestIdBuilder.get());
      request.setServiceName("DIH");
      request.setServiceVersion("2.3.0-internal");
      client.activate(request);
    } catch (Exception e) {
      System.out.println("exception occur" + e);
    }
  }

  public void getStatus() {

  }

  /**
   * xx.
   */
  public void deactivate() {
    try {
      DeploymentDaemon.Iface client = deploymentDaemonClientFactory.build("10.0.1.112", 10002)
          .getClient();
      DeactivateRequest request = new DeactivateRequest();
      request.setRequestId(RequestIdBuilder.get());
      request.setServiceName("DIH");
      client.deactivate(request);
    } catch (Exception e) {
      System.out.println("exception occur" + e);
    }
  }

}
