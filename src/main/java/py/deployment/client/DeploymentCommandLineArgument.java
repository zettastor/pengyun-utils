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

package py.deployment.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import py.common.PyService;
import py.deployment.common.DeploymentOperation;
import py.instance.Group;

/**
 * A class holds all arguments given in command line.
 */
@Configuration
public class DeploymentCommandLineArgument {

  private static final Logger logger = LoggerFactory.getLogger(DeploymentCommandLineArgument.class);

  /**
   * Specify deployment operation, all operations are defined in {@link DeploymentOperation}.
   */
  @Value("${operation}")
  private String operation;

  /**
   * Specify which service is operated on. All services are defined in {@link PyService}.
   */
  @Value("${serviceName}")
  private String serviceName;

  /**
   * Specify the range of deployment host. The range should use format
   * "fromIp:toIp,Ip,fromIp:toIp,...".
   */
  @Value("${serviceHostRange:}")
  private String serviceHostRange;

  /**
   * Add other params to each operation.
   */
  @Value("${params:}")
  private String params;

  /**
   * Specify which group is operated on.
   */
  @Value("${groupId:}")
  private String groupId;

  @Bean
  public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
    return new PropertySourcesPlaceholderConfigurer();
  }

  /**
   * xx.
   */
  public DeploymentOperation getOperation() {
    Validate.notNull(operation);
    DeploymentOperation targetop = DeploymentOperation.findValueByName(operation);

    if (null == targetop) {
      logger.error("Unrecognized operation {}, the supported operations are {}", operation,
          DeploymentOperation.values());
      throw new IllegalArgumentException("Unrecognized operation " + operation);
    }

    return targetop;
  }

  /**
   * xx.
   */
  public List<String> getParams() {
    if (params == null || params.isEmpty()) {
      return null;
    }

    return Arrays.asList(params.split(","));
  }

  /**
   * xx.
   */
  public Group getGroup() {
    if (groupId == null || groupId.isEmpty()) {
      return null;
    }

    return new Group(Integer.valueOf(groupId));
  }


  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  /**
   * xx.
   */
  public String getServiceHostRange() {
    if (serviceHostRange == null || serviceHostRange.isEmpty()) {
      return null;
    }

    return serviceHostRange;
  }

  /**
   * xx.
   */
  public List<String> getServiceNameList() {
    Validate.notNull(serviceName);

    List<String> serviceNameList = new ArrayList<String>();
    String upperCaseServiceName = serviceName.toUpperCase();

    if (upperCaseServiceName.equals("ALL")) {
      return listAllServiceName(getOperation());
    }

    PyService targetService = PyService.findValueByName(upperCaseServiceName);
    if (targetService == null) {
      Set pyServiceSet = new HashSet();
      for (PyService pyService : PyService.values()) {
        if (pyService != PyService.DEPLOYMENTDAMON) {
          logger.info("add service name: {}", pyService.getServiceName());
          pyServiceSet.add(pyService);
        }
      }

      logger.error("Invalid service name: {}, all valid service names are : {}", serviceName,
          pyServiceSet);
      throw new IllegalArgumentException("Unrecognized service " + serviceName);
    }
    serviceNameList.add(PyService.findValueByName(upperCaseServiceName).getServiceName());

    logger.info("All service name: {}", serviceNameList);
    return serviceNameList;
  }

  private List<String> listAllServiceName(DeploymentOperation deploymentOp) {
    List<String> serviceNameList = new ArrayList<String>();

    switch (deploymentOp) {
      case WIPEOUT:
        serviceNameList.add(PyService.DRIVERCONTAINER.getServiceName());
        serviceNameList.add(PyService.COORDINATOR.getServiceName());
        serviceNameList.add(PyService.DIH.getServiceName());
        serviceNameList.add(PyService.INFOCENTER.getServiceName());
        for (PyService pyService : PyService.values()) {
          // PyService.COORDINATOR == pyService ||

          if (serviceNameList.contains(pyService.getServiceName())
              || pyService == PyService.DEPLOYMENTDAMON) {
            continue;
          }

          serviceNameList.add(pyService.getServiceName());
        }
        logger.info("operation: WIPEOUT, all service name: {}", serviceNameList);
        return serviceNameList;
      default:
        serviceNameList.add(PyService.DIH.getServiceName());
        serviceNameList.add(PyService.INFOCENTER.getServiceName());
        serviceNameList.add(PyService.COORDINATOR.getServiceName());
        serviceNameList.add(PyService.DRIVERCONTAINER.getServiceName());
        for (PyService pyService : PyService.values()) {
          // PyService.COORDINATOR == pyService ||

          if (serviceNameList.contains(pyService.getServiceName())
              || pyService == PyService.DEPLOYMENTDAMON) {
            logger.info("skip service name: {}", pyService.getServiceName());
            continue;
          }
          serviceNameList.add(pyService.getServiceName());
        }
        logger.info("operation: {}, all service name: {}", deploymentOp, serviceNameList);
        return serviceNameList;
    }
  }
}
