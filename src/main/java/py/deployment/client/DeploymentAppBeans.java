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

package py.deployment.client;

import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.DeploymentDaemonClientFactory;
import py.common.PyService;
import py.deployment.common.DeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.ConsoleDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.CoordinatorDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DdDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DihDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DriverContainerDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.InfoCenterDeploymentConfiguration;
import py.deployment.common.DeploymentConfigurationFactory;
import py.dih.client.DihClientFactory;

/**
 * A class holds all beans used in deployment base on spring java configuration framework.
 */
@Import({DihDeploymentConfiguration.class, InfoCenterDeploymentConfiguration.class,
    ConsoleDeploymentConfiguration.class,
    DataNodeDeploymentConfiguration.class,
    DriverContainerDeploymentConfiguration.class,
    CoordinatorDeploymentConfiguration.class, DdDeploymentConfiguration.class})
@Configuration
public class DeploymentAppBeans {

  @Autowired
  private DihDeploymentConfiguration dihDeploymentConfiguration;

  @Autowired
  private InfoCenterDeploymentConfiguration infoCenterDeploymentConfiguration;

  @Autowired
  private ConsoleDeploymentConfiguration consoleDeploymentConfiguration;

  @Autowired
  private DataNodeDeploymentConfiguration dataNodeDeploymentConfiguration;


  @Autowired
  private DriverContainerDeploymentConfiguration driverContainerDeploymentConfiguration;

  @Autowired
  private CoordinatorDeploymentConfiguration coordinatorDeploymentConfiguration;

  @Autowired
  private DdDeploymentConfiguration ddDeploymentConfiguration;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  @Bean
  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory();
    return dihClientFactory;
  }

  /**
   * Prepare configurations for each service in pengyun storage service to factory.
   */
  @Bean
  public DeploymentConfigurationFactory deploymentConfigurationFactory() {
    Map<String, DeploymentConfiguration> serviceName2DeploymentConfiguration =
        new HashMap<String, DeploymentConfiguration>();
    serviceName2DeploymentConfiguration
        .put(PyService.DIH.getServiceName(), dihDeploymentConfiguration);
    serviceName2DeploymentConfiguration
        .put(PyService.CONSOLE.getServiceName(), consoleDeploymentConfiguration);
    serviceName2DeploymentConfiguration.put(PyService.INFOCENTER.getServiceName(),
        infoCenterDeploymentConfiguration);
    serviceName2DeploymentConfiguration.put(PyService.DRIVERCONTAINER.getServiceName(),
        driverContainerDeploymentConfiguration);
    serviceName2DeploymentConfiguration
        .put(PyService.DATANODE.getServiceName(), dataNodeDeploymentConfiguration);
    serviceName2DeploymentConfiguration.put(PyService.COORDINATOR.getServiceName(),
        coordinatorDeploymentConfiguration);
    serviceName2DeploymentConfiguration.put(PyService.DEPLOYMENTDAMON.getServiceName(),
        ddDeploymentConfiguration);

    DeploymentConfigurationFactory deploymentConfigurationFactory =
        new DeploymentConfigurationFactory(
            serviceName2DeploymentConfiguration);
    return deploymentConfigurationFactory;
  }

  /**
   * xx.
   */
  @Bean
  public DeploymentDaemonClientFactory deploymentDaemonClientFactory() {
    DeploymentDaemonClientFactory deploymentDaemonClientFactory =
        new DeploymentDaemonClientFactory();
    return deploymentDaemonClientFactory;
  }

  /**
   * xx.
   */
  @Bean
  public Deployment deployment() {
    DeploymentImpl deploymentImpl = new DeploymentImpl(deploymentConfigurationFactory(),
        deploymentDaemonClientFactory());
    deploymentImpl.setDihClientFactory(dihClientFactory());
    return deploymentImpl;
  }
}
