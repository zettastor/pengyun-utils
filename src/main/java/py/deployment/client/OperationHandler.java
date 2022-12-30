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

import static py.common.PyService.DATANODE;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import py.common.PyService;
import py.dd.common.ServiceMetadata;
import py.deployment.common.DeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;
import py.deployment.common.DeploymentConfigurationFactory;
import py.deployment.common.DeploymentOperation;
import py.instance.Group;

/**
 * A class whose instance is command line deployment client. The instance accepts arguments from
 * command line, parse them, and operate on these info.
 */
public class OperationHandler {

  private static final Logger logger = LoggerFactory.getLogger(OperationHandler.class);
  private static final File resultFile = new File("deployment-result.xml");
  private static final String WITHOUT_CONFIG = "withoutconfig";

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (resultFile.exists()) {
      resultFile.delete();
    }

    // parse command line argument
    PropertySource propertySource = new SimpleCommandLinePropertySource(args);
    AnnotationConfigApplicationContext commandLineContext =
        new AnnotationConfigApplicationContext();
    commandLineContext.getEnvironment().getPropertySources().addFirst(propertySource);
    commandLineContext.register(DeploymentCommandLineArgument.class);
    commandLineContext.refresh();

    DeploymentCommandLineArgument commandLineArgument = commandLineContext
        .getBean(DeploymentCommandLineArgument.class);

    ApplicationContext appContex = new AnnotationConfigApplicationContext(DeploymentAppBeans.class);
    Deployment deployment = appContex.getBean(Deployment.class);
    DeploymentConfigurationFactory configFactory = appContex
        .getBean(DeploymentConfigurationFactory.class);
    if (commandLineArgument.getParams() != null) {
      for (String param : commandLineArgument.getParams()) {
        configFactory.addParam(param);
      }
    }

    List<String> serviceList = commandLineArgument.getServiceNameList();
    if (commandLineArgument.getServiceHostRange() != null) {
      if (commandLineArgument.getOperation() != DeploymentOperation.WIPEOUT) {
        Iterator<String> iterator = serviceList.iterator();
        while (iterator.hasNext()) {
          String serviceName = iterator.next();
          DeploymentConfiguration config = configFactory.getDeploymentConfiguration(serviceName);
          try {
            List<String> range = config.getServiceDeploymentHosts();
            List<String> filtratRange = config
                .getAllHostsInRange(commandLineArgument.getServiceHostRange());
            logger.debug("for service {}, range {}, filtratrange {}", serviceName, range,
                filtratRange);
            boolean keep = false;
            for (String host : range) {
              if (filtratRange.contains(host)) {
                keep = true;
                break;
              }
            }
            if (!keep) {
              iterator.remove();
            }
          } catch (UnknownHostException e) {
            Validate.isTrue(false, "invalidate host");
          }
        }
      }

      // use service host range specified in command line
      configFactory.setHostRange(commandLineArgument.getServiceHostRange());
    }
    configFactory.setGroup(commandLineArgument.getGroup());

    boolean operationDone = true;
    boolean isDriver = false;
    boolean isSkipped = false;
    //If change dd.properties host infomation 
    // for some service after deploy all service ,and now dd will use the new host
    // infomation to wipeout the service,and the old service 
    // will not be wipeout. Use without_config option with "-d" in
    // commandLine,and then wipeout all 
    // service in defalut running (/var/testing/packages) in dd hosts .
    if (commandLineArgument.getOperation() == DeploymentOperation.WIPEOUT && commandLineArgument
        .getServiceName().toUpperCase().equals("ALL")
        && commandLineArgument.getParams() != null && commandLineArgument.getParams()
        .contains(WITHOUT_CONFIG)) {
      DeploymentConfiguration config = configFactory
          .getDeploymentConfiguration(PyService.DEPLOYMENTDAMON.getServiceName());
      logger.warn("going to wipeout all service which in dd host :{}",
          config.getServiceDeploymentHosts());
      operationDone = deployment.wipeout(config);
    } else {
      for (String serviceName : serviceList) {

        logger.info("operation: {} serviceName : {}", commandLineArgument.getOperation().name(),
            serviceName);
        isDriver = PyService.findValueByServiceName(serviceName).getServiceLauchingScriptName()
            .isEmpty();
        DeploymentConfiguration configuration = configFactory
            .getDeploymentConfiguration(serviceName);

        // if configed not deploy should not deploy
        boolean deployFlag = configuration.isDeployEnabled();
        if (deployFlag == false) {
          logger.info("Service {} deploy config {} ", serviceName, deployFlag);
          continue;
        }

        deployment.init(serviceName, configuration.getServicePort());
        boolean operationDoneForService = false;

        logger
            .info("Going to {} service {}", commandLineArgument.getOperation().name(), serviceName);

        Map<Integer, List<String>> groupId2Hosts = null;
        if (serviceName.equals(DATANODE.getServiceName())) {
          DataNodeDeploymentConfiguration config = (DataNodeDeploymentConfiguration) configFactory
              .getDeploymentConfiguration(serviceName);
          if ((commandLineArgument.getServiceHostRange() == null) && config.isGroupEnabled()
              && (commandLineArgument.getGroup() == null)) {
            logger.warn("*** Using group assignment in configuration."
                + "Please make sure it is as same as "
                + "assignment already exists in system if this is not the first deployment.");
            groupId2Hosts = config.parseGroupDeploymentHosts();
          }
        }

        switch (commandLineArgument.getOperation()) {

          case ACTIVATE:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }

            if (groupId2Hosts != null) {
              logger.info("Group in configuration file is enabled, detail info: {}", groupId2Hosts);
              for (int id : groupId2Hosts.keySet()) {
                configFactory.setGroup(new Group(id));
                configFactory.addParam("group" + id);
                operationDoneForService = deployment.activate();
                DeploymentConfiguration config = configFactory
                    .getDeploymentConfiguration(serviceName);
                List<String> paramList = config.getParamList();
                for (int i = 0; i < paramList.size(); i++) {
                  if (paramList.get(i) == ("group" + id)) {
                    paramList.remove(i);
                    i--;
                  }

                }
              }
            } else {
              operationDoneForService = deployment.activate();
            }
            break;

          case DEACTIVATE:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }
            operationDoneForService = deployment.deactivate();
            break;
          case DEPLOY:
            if (groupId2Hosts != null) {
              logger.info("Group in configuration file is enabled, detail info: {}", groupId2Hosts);
              for (int id : groupId2Hosts.keySet()) {
                configFactory.setGroup(new Group(id));
                configFactory.addParam("group" + id);
                operationDoneForService = deployment.deploy();
                DeploymentConfiguration config = configFactory
                    .getDeploymentConfiguration(serviceName);
                List<String> paramList = config.getParamList();
                for (int i = 0; i < paramList.size(); i++) {
                  if (paramList.get(i) == ("group" + id)) {
                    paramList.remove(i);
                    i--;
                  }
                }
              }
            } else {
              operationDoneForService = deployment.deploy();
            }
            break;
          case DESTROY:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }
            operationDoneForService = deployment.destroy();
            break;
          case RESTART:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }
            operationDoneForService = deployment.restart();
            break;
          case START:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }
            if (groupId2Hosts != null) {
              logger.info("Group in configuration file is enabled, detail info: {}", groupId2Hosts);
              for (int id : groupId2Hosts.keySet()) {
                configFactory.setGroup(new Group(id));
                configFactory.addParam("group" + id);
                operationDoneForService = deployment.start();
                DeploymentConfiguration config = configFactory
                    .getDeploymentConfiguration(serviceName);
                List<String> paramList = config.getParamList();
                for (int i = 0; i < paramList.size(); i++) {
                  if (paramList.get(i) == ("group" + id)) {
                    paramList.remove(i);
                    i--;
                  }
                }
              }
            } else {
              operationDoneForService = deployment.start();
            }
            break;
          case STATUS:
            if (isDriver) {
              logger
                  .warn("Service {} is actually a driver, operation {} is not supported, skip it!",
                      serviceName,
                      commandLineArgument.getOperation().name());
              isSkipped = true;
              continue;
            }
            Map<String, ServiceMetadata> host2Service = deployment.status();
            operationDoneForService = (host2Service == null) ? false : true;
            if (operationDoneForService) {
              for (String serviceHost : host2Service.keySet()) {
                ServiceMetadata service = host2Service.get(serviceHost);
                logger.info("service {} on host {} is {}", serviceName, serviceHost,
                    service.getServiceStatus().name());
              }
            }
            break;
          case UPGRADE:
            operationDoneForService = deployment.upgrade();
            break;
          case WIPEOUT:
            if (groupId2Hosts != null) {
              logger.info("Group in configuration file is enabled, detail info: {}", groupId2Hosts);
              for (int id : groupId2Hosts.keySet()) {
                configFactory.setGroup(new Group(id));
                configFactory.addParam("group" + id);
                operationDoneForService = deployment.wipeout();
                DeploymentConfiguration config = configFactory
                    .getDeploymentConfiguration(serviceName);
                List<String> paramList = config.getParamList();
                for (int i = 0; i < paramList.size(); i++) {
                  if (paramList.get(i) == ("group" + id)) {
                    paramList.remove(i);
                    i--;
                  }
                }
              }
            } else {
              operationDoneForService = deployment.wipeout();
            }
            break;
          case CONFIGURE:
            operationDoneForService = deployment.configure();
            break;
          default:

        }

        operationDone = operationDone && operationDoneForService;
        Map<DeploymentOperation, List<String>> failureTable = deployment.getFailures();
        if (!operationDoneForService) {
          logger.error("Unable to complete operation {} for service {}",
              commandLineArgument.getOperation().name(), serviceName);
          reportFalure(serviceName, failureTable);
          continue;
        }

        Validate.isTrue(failureTable.isEmpty());
      }
    }
    if (!operationDone) {
      logger.debug("Something wrong in deployment! See detail in log and {}", resultFile);
      System.exit(1);
    } else {
      if (isDriver && isSkipped) {
        logger.info("Service {} is driver,can not {},so it has been skipped succesfully!",
            serviceList,
            commandLineArgument.getOperation().name());
      } else {
        logger.info("Successfully to complete operation {} on service {}",
            commandLineArgument.getOperation().name(), serviceList);
      }
      System.exit(0);
    }
  }

  /**
   * xx.
   */
  public static void reportFalure(String service,
      Map<DeploymentOperation, List<String>> failureTable) {
    StringBuilder builder = new StringBuilder();
    builder.append("<Service name=\"" + service + "\">\n");
    for (DeploymentOperation operation : failureTable.keySet()) {
      builder.append("    <Deployment-Operation name=\"" + operation.name() + "\">\n");
      for (String host : failureTable.get(operation)) {
        builder.append("        <Host name=\"" + host + "\" />\n");
      }
      builder.append("    </Deployment-Operation>\n");
    }
    builder.append("</Service>\n");

    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(resultFile, true));
      writer.write(builder.toString());
    } catch (Exception e) {
      logger.error("Caught an exception when write result", e);
    } finally {
      if (null != writer) {
        try {
          writer.close();
        } catch (IOException e) {
          logger.error("", e);
        }
      }
    }
  }
}
