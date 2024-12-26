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
