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
