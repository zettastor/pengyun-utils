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

package py.deployment.common;

import java.util.Map;
import py.instance.Group;

/**
 * xx.
 */
public class DeploymentConfigurationFactory {

  Map<String, DeploymentConfiguration> serviceName2DeploymentConfiguration;

  /**
   * xx.
   */
  public DeploymentConfigurationFactory(
      Map<String, DeploymentConfiguration> serviceName2DeploymentConfiguration) {
    this.serviceName2DeploymentConfiguration = serviceName2DeploymentConfiguration;

    for (String serviceName : serviceName2DeploymentConfiguration.keySet()) {
      DeploymentConfiguration deploymentConfiguration = serviceName2DeploymentConfiguration
          .get(serviceName);
      deploymentConfiguration.initialize();
    }
  }

  public DeploymentConfiguration getDeploymentConfiguration(String serviceName) {
    return serviceName2DeploymentConfiguration.get(serviceName);
  }

  /**
   * xx.
   */
  public DeploymentConfigurationFactory setGroup(Group group) {
    for (String serviceName : serviceName2DeploymentConfiguration.keySet()) {
      DeploymentConfiguration config = serviceName2DeploymentConfiguration.get(serviceName);
      config.setGroup(group);
    }

    return this;
  }

  /**
   * xx.
   */
  public DeploymentConfigurationFactory addParam(String param) {
    for (String serviceName : serviceName2DeploymentConfiguration.keySet()) {
      DeploymentConfiguration config = serviceName2DeploymentConfiguration.get(serviceName);
      config.addParam(param);
    }

    return this;
  }

  /**
   * xx.
   */
  public DeploymentConfigurationFactory setHostRange(String hostRange) {
    for (String serviceName : serviceName2DeploymentConfiguration.keySet()) {
      DeploymentConfiguration config = serviceName2DeploymentConfiguration.get(serviceName);
      config.setServiceHostRange(hostRange);
      config.initialize();
    }

    return this;
  }
}
