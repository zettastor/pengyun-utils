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

import py.DeploymentDaemonClientFactory;

/**
 * deploymentdaemon client main
 *
 * <p>you can through dd.sh to execute put, activate, getstatus, deactivate at command line.
 *
 * <p>DeploymentDaemon receives command from command line and then call DeploymentDaemonClient.
 */
public class DeploymentDaemon {

  /**
   * xx.
   */
  public static void main(String[] args) {
    DeploymentDaemonClientFactory deploymentDaemonClientFactory = null;
    try {
      deploymentDaemonClientFactory = new DeploymentDaemonClientFactory(1);
      DeploymentDaemonClientHandler ddClient = new DeploymentDaemonClientHandler();
      ddClient.setDeploymentDaemonClientFactory(deploymentDaemonClientFactory);
      ddClient.deactivate();
    } finally {
      if (deploymentDaemonClientFactory != null) {
        deploymentDaemonClientFactory.close();
      }
    }
  }

}
