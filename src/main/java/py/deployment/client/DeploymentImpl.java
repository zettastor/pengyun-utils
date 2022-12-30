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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.DeploymentDaemonClientFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.dd.DeploymentDaemonClientWrapper;
import py.dd.client.exception.DriverIsAliveException;
import py.dd.client.exception.FailedToWipeoutException;
import py.dd.client.exception.ServiceIsBusyException;
import py.dd.client.exception.ServiceNotFoundException;
import py.dd.client.exception.ServiceStatusIsErrorException;
import py.dd.common.ServiceMetadata;
import py.dd.common.ServiceStatus;
import py.deployment.common.DeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DihDeploymentConfiguration;
import py.deployment.common.DeploymentConfigurationFactory;
import py.deployment.common.DeploymentOperation;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.service.configuration.manager.XmlConfigurationFileReader;
import py.thrift.deploymentdaemon.PrepareWorkspaceRequest;
import py.thrift.deploymentdaemon.ServiceNotRunnableExceptionThrift;
import py.thrift.deploymentdaemon.UpdateLatestVersionRequest;

/**
 * A class includes handler of deployment operation base on deployment configuration.
 */
public class DeploymentImpl implements Deployment {

  private static final Logger logger = LoggerFactory.getLogger(DeploymentImpl.class);
  private final List<DeploymentOperation> operationList = new ArrayList<DeploymentOperation>();
  private final Map<DeploymentOperation, List<String>> failureTalbe =
      new HashMap<DeploymentOperation, List<String>>();
  /**
   * A factory to build dih client from which all instances info could be acquired.
   */
  private DihClientFactory dihClientFactory;
  /**
   * A factory to build dd client from which operation is sent to dd server.
   */
  private DeploymentDaemonClientFactory ddClientFactory;
  /**
   * A factory to build configuration for a given service.
   */
  private DeploymentConfigurationFactory deploymentConfigurationFactory;
  private String serviceName;
  private String errorCause;
  private int servicePort;
  private String timestamp = null;
  private String coordinatorTimestamp = null;
  private String fsserverTimestamp = null;

  public DeploymentImpl(DeploymentConfigurationFactory configurationFactory,
      DeploymentDaemonClientFactory ddClientFactory) {
    this.deploymentConfigurationFactory = configurationFactory;
    this.ddClientFactory = ddClientFactory;
  }

  public int getPort() {
    return servicePort;
  }

  public void setPort(int port) {
    this.servicePort = port;
  }

  public String getErrorCause() {
    return errorCause;
  }

  public void setErrorCause(String errorCause) {
    this.errorCause = errorCause;
  }

  /**
   * xx.
   */
  public void init(String serviceName, int servicePort) {
    this.serviceName = serviceName;
    this.servicePort = servicePort;
    if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
      this.coordinatorTimestamp = getTimestamp(PyService.COORDINATOR.getServiceName());
    } else if (serviceName.equals(PyService.DRIVERCONTAINER.getServiceName())) {
      this.coordinatorTimestamp = getDriverTimestamp(PyService.COORDINATOR.getServiceName());
    } 
    logger.debug("fsserverTimestamp:{} for service:{}", fsserverTimestamp, serviceName);
    logger.debug("coordinatorTimestamp:{} for service:{}", coordinatorTimestamp, serviceName);
    operationList.clear();
    failureTalbe.clear();
  }

  /**
   * xx.
   */
  public String getTimestamp(String name) {
    final DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);
    String[] packageNames = config.getPackagesPath().toFile().list();
    String timestamp = null;
    boolean packageExist = false;
    if (serviceName.equals(name)) {
      timestamp = config.getServiceTimestamp();
      logger.warn("getTimestamp: {} of serviceName {}", timestamp, serviceName);
      // check package exists
      for (String packageName : packageNames) {
        if (packageName
            .contains(PyService.findValueByServiceName(serviceName).getServiceProjectKeyName())
            && packageName.contains(timestamp)) {
          packageExist = true;
          break;
        }
      }
    }
    if (!packageExist) {
      logger.error("please check timestamp config and packages of {}", serviceName);
      return null;
    }

    logger.debug("getCoordinatorTimestamp {}", timestamp);
    return timestamp;
  }

  /**
   * xx.
   */
  public String getDriverTimestamp(String name) {
    final DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(name);
    String timestamp = config.getServiceTimestamp();
    logger.debug("service {} timestamp is:{}", name, timestamp);
    return timestamp;
  }

  public DihClientFactory getDihClientFactory() {
    return dihClientFactory;
  }

  public void setDihClientFactory(DihClientFactory dihClientFactory) {
    this.dihClientFactory = dihClientFactory;
  }

  @Override
  public boolean deploy() {
    boolean success = transfer();
    operationList.add(DeploymentOperation.TRANSFER);

    PyService service = PyService.findValueByServiceName(serviceName);
    if (service.getServiceLauchingScriptName().isEmpty()) {
      success = prepareWorkspace() && success;
    } else {
      success = activate() && success;
      if (serviceName.equals(PyService.DRIVERCONTAINER.getServiceName())) {
        success = success && updateLatestVersion();
      }
    }
    return success;
  }

  @Override
  public boolean upgrade() {
    boolean success = true;

    PyService service = PyService.findValueByServiceName(serviceName);
    if (serviceName.equals(PyService.CONSOLE.getServiceName())) {
      success = destroy() && success;
      operationList.add(DeploymentOperation.DESTROY);

      success = deploy() && success;
      return success;
    } else if (serviceName.equals(PyService.INFOCENTER.getServiceName())) {
      success = backupKey() && success;
      operationList.add(DeploymentOperation.BACKUP_KEY);

      success = deactivate() && success;
      operationList.add(DeploymentOperation.DEACTIVATE);

      success = transfer() && success;
      operationList.add(DeploymentOperation.TRANSFER);

      success = useBackupKey() && success;
      operationList.add(DeploymentOperation.USE_BACKUP_KEY);

      success = activate() && success;
      return success;
    } else if (service.getServiceLauchingScriptName().isEmpty()) {
      success = deploy() && success;

      if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
        success = checkUpgradeStatus();
      }
      return success;
    } else {
      success = deactivate() && success;
      operationList.add(DeploymentOperation.DEACTIVATE);

      success = deploy() && success;
      return success;
    }
  }

  @Override
  public boolean wipeout() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneWipeout = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort(),
            config.getOperationTimeoutMs());

        logger.info("This operation may take some minutes to finish, please wait ...");
        if (serviceName.equals(PyService.DRIVERCONTAINER.getServiceName()) || serviceName
            .equals(PyService.COORDINATOR.getServiceName())) {
          ddClient.wipeout(serviceName, coordinatorTimestamp, coordinatorTimestamp,
              config.getPackageVersion());
        } else {
          ddClient.wipeout(serviceName);
        }

      } catch (DriverIsAliveException e) {
        logger.error("Driver process is alive, unlable to wipeout {} on remote {}", serviceName,
            deploymentHost,
            e);
        doneWipeout = false;
        addFailureHost(DeploymentOperation.WIPEOUT, deploymentHost);
        continue;
      } catch (FailedToWipeoutException e) {
        logger.warn("Wipeout {} failed", serviceName, e);
        doneWipeout = false;
        addFailureHost(DeploymentOperation.WIPEOUT, deploymentHost);
        continue;
      } catch (Exception e) {
        logger.error("Caught an exception when destroy service {} on remote {}", serviceName,
            deploymentHost, e);
        doneWipeout = false;
        addFailureHost(DeploymentOperation.WIPEOUT, deploymentHost);
        continue;
      }
    }
    return doneWipeout;
  }

  /**
   * Get all hosts infomation for dd service, and then wipeout all service in those hosts,instead of
   * use service host infomation from  dd.properties.
   */

  public boolean wipeout(DeploymentConfiguration config) {
    List<String> ddHosts = config.getServiceDeploymentHosts();
    if (ddHosts == null) {
      logger.error("Something wrong when pick hosts of DeploymentDaemon");
      return false;
    }
    boolean doneWipeout = true;
    for (String deploymentHost : ddHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort(),
            config.getOperationTimeoutMs());
        logger.info("This operation may take some minutes to finish, please wait ...");
        ddClient.wipeout();
      } catch (DriverIsAliveException e) {
        logger.error("Driver process is alive,unlable to wipeout {} on remote {}", serviceName,
            deploymentHost, e);
        doneWipeout = false;
        addFailureHost(DeploymentOperation.WIPEOUT, deploymentHost);
        continue;
      } catch (Exception e) {
        logger.error("Caught an exception when destroy service {} on remote {}", serviceName,
            deploymentHost, e);
        doneWipeout = false;
        addFailureHost(DeploymentOperation.WIPEOUT, deploymentHost);
        continue;
      }
    }
    return doneWipeout;
  }


  @Override
  public boolean destroy() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneDestroy = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        ddClient.destroy(serviceName);
      } catch (ServiceIsBusyException e) {
        logger.error("Service {} on remote {} is busy, unable to destroy it", serviceName,
            deploymentHost, e);
        doneDestroy = false;
        addFailureHost(DeploymentOperation.DESTROY, deploymentHost);
        continue;

      } catch (Exception e) {
        logger.error("Caught an exception when destroy service {} on remote {}", serviceName,
            deploymentHost,
            e);
        doneDestroy = false;
        addFailureHost(DeploymentOperation.DESTROY, deploymentHost);
        continue;
      }
    }
    return doneDestroy;
  }

  @Override
  public boolean activate() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    boolean doneActive = true;
    for (int step = 0; step < 2; step++) {
      List<String> deploymentHosts = new ArrayList<String>();

      switch (step) {
        case 0:
          if (!config.getServiceName().equals(PyService.DIH.getServiceName())) {
            // skip step 0 to step 1 if not dih service
            continue;
          }
          DihDeploymentConfiguration dihConfig = (DihDeploymentConfiguration) config;

          List<String> allDeploymentHosts = pickServiceHostByGroup(serviceName);
          for (String host : dihConfig.getCenterDihHosts()) {
            if (allDeploymentHosts.contains(host)) {
              deploymentHosts.add(host);
            }
          }
          break;
        case 1:
          deploymentHosts = pickServiceHostByGroup(serviceName);
          break;
        default:
          break;
      }

      if (deploymentHosts == null) {
        logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
            config.getGroup());
        return false;
      }

      for (String deploymentHost : deploymentHosts) {
        DeploymentDaemonClientWrapper ddClient = null;
        try {
          ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
          for (String param : config.getParamList()) {
            ddClient.attachParam(param);
          }

          ddClient.activate(serviceName, config.getPackageVersion());
        } catch (ServiceIsBusyException e) {
          logger.error("Service {} on remote {} is busy, unable to activate it", serviceName,
              deploymentHost);
          doneActive = false;
          if (config.getServiceName().equals(PyService.DIH.getServiceName()) && 0 == step) {
            addFailureHosts(DeploymentOperation.ACTIVATE, config.getServiceDeploymentHosts());
            return doneActive;
          }
          addFailureHost(DeploymentOperation.ACTIVATE, deploymentHost);
          continue;
        } catch (ServiceStatusIsErrorException e) {
          logger.error(
              "Service {} on remote {} status is ERROR, unable to activate,it should be destroyed",
              serviceName, deploymentHost);
          doneActive = false;
          if (config.getServiceName().equals(PyService.DIH.getServiceName()) && 0 == step) {
            addFailureHosts(DeploymentOperation.ACTIVATE, config.getServiceDeploymentHosts());
            return doneActive;
          }
          addFailureHost(DeploymentOperation.ACTIVATE, deploymentHost);
          continue;
        } catch (Exception e) {
          logger.error("Caught an exception when activate service {} on remote {}", serviceName,
              deploymentHost, e);
          doneActive = false;
          if (config.getServiceName().equals(PyService.DIH.getServiceName()) && 0 == step) {
            addFailureHosts(DeploymentOperation.ACTIVATE, config.getServiceDeploymentHosts());
            return doneActive;
          }
          addFailureHost(DeploymentOperation.ACTIVATE, deploymentHost);
          continue;
        }
      }

      List<String> checkHosts;
      if (0 == step) {
        checkHosts = deploymentHosts;
      } else {
        List<DeploymentOperation> tmpOpList = new ArrayList<>(operationList);
        tmpOpList.add(DeploymentOperation.ACTIVATE);
        checkHosts = pickServiceHostByGroup(serviceName, tmpOpList);
      }

      List<String> pendingHosts = waitUntilServiceOnAllHostsTurnInto(serviceName,
          ServiceStatus.ACTIVE,
          checkHosts, config.getDeploymentDaemonPort(), config.getOperationTimeoutMs());
      if (!pendingHosts.isEmpty()) {
        addFailureHosts(DeploymentOperation.ACTIVATE, pendingHosts);
        return false;
      }
    }
    return doneActive;
  }

  @Override
  public boolean deactivate() {
    if (serviceName.equals(PyService.CONSOLE.getServiceName())) {
      return destroy();
    }

    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneDeactivate = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        ddClient.deactivate(serviceName, servicePort);
      } catch (ServiceIsBusyException e) {
        logger.error("Service {} on remote {} is busy, unable to deactivate it", serviceName,
            deploymentHost);
        doneDeactivate = false;
        addFailureHost(DeploymentOperation.DEACTIVATE, deploymentHost);
      } catch (ServiceStatusIsErrorException e) {
        logger.error(
            "Service {} on remote {} status is ERROR, unable to deactivate,it should be destroyed",
            serviceName, deploymentHost);
        doneDeactivate = false;
        addFailureHost(DeploymentOperation.DEACTIVATE, deploymentHost);
      } catch (Exception e) {
        logger.error("Caught an exception when deactivate service {} on remote {}", serviceName,
            deploymentHost,
            e);
        doneDeactivate = false;
        addFailureHost(DeploymentOperation.DEACTIVATE, deploymentHost);
        continue;
      }
    }

    List<DeploymentOperation> tmpOpList = new ArrayList<>(operationList);
    tmpOpList.add(DeploymentOperation.DEACTIVATE);
    List<String> checkHosts = pickServiceHostByGroup(serviceName, tmpOpList);

    List<String> pendingHosts = waitUntilServiceOnAllHostsTurnInto(serviceName,
        ServiceStatus.DEACTIVE, checkHosts,
        config.getDeploymentDaemonPort(), config.getOperationTimeoutMs());

    if (!pendingHosts.isEmpty()) {
      doneDeactivate = false;
      addFailureHosts(DeploymentOperation.DEACTIVATE, pendingHosts);
    }

    return doneDeactivate;
  }

  @Override
  public boolean start() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneStart = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        for (String param : config.getParamList()) {
          ddClient.attachParam(param);
        }
        ddClient.start(serviceName);
      } catch (ServiceIsBusyException e) {
        logger.error("Service {} on remote {} is busy, unable to start it", serviceName,
            deploymentHost);
        doneStart = false;
        addFailureHost(DeploymentOperation.START, deploymentHost);
        continue;
      } catch (ServiceStatusIsErrorException e) {
        logger.error(
            "Service {} on remote {} status is ERROR, unable to start,it should be destroyed",
            serviceName, deploymentHost);
        doneStart = false;
        addFailureHost(DeploymentOperation.START, deploymentHost);
        continue;
      } catch (Exception e) {
        logger.error("Caught an exception when start service {} on remote {}", serviceName,
            deploymentHost, e);
        doneStart = false;
        addFailureHost(DeploymentOperation.START, deploymentHost);
        continue;
      }
    }
    List<DeploymentOperation> tmpOpList = new ArrayList<>(operationList);
    tmpOpList.add(DeploymentOperation.START);
    List<String> checkHosts = pickServiceHostByGroup(serviceName, tmpOpList);

    List<String> pendingHosts = waitUntilServiceOnAllHostsTurnInto(serviceName,
        ServiceStatus.ACTIVE, checkHosts,
        config.getDeploymentDaemonPort(), config.getOperationTimeoutMs());
    if (!pendingHosts.isEmpty()) {
      addFailureHosts(DeploymentOperation.START, pendingHosts);
      return false;
    }
    return doneStart;
  }

  @Override
  public boolean restart() {
    boolean success = deactivate();
    operationList.add(DeploymentOperation.DEACTIVATE);

    success = start() && success;
    return success;
  }

  @Override
  public boolean configure() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    XmlConfigurationFileReader xmlReader = new XmlConfigurationFileReader(
        config.getXmlConfigurationPath());
    Map<String, Properties> propsFileName2Properties = xmlReader
        .getProperties(PyService.findValueByServiceName(serviceName).getServiceProjectKeyName());

    boolean doneConfigure = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        for (String propsFileName : propsFileName2Properties.keySet()) {
          ddClient
              .configure(serviceName, propsFileName, propsFileName2Properties.get(propsFileName));
        }
      } catch (Exception e) {
        logger.error("Caught an exception when destroy service {} on remote {}", serviceName,
            deploymentHost,
            e);
        doneConfigure = false;
        addFailureHost(DeploymentOperation.CONFIGURE, deploymentHost);
        continue;
      }
    }

    return doneConfigure;
  }

  @Override
  public Map<String, ServiceMetadata> status() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return null;
    }

    Map<String, ServiceMetadata> deploymentHost2Service = new HashMap<String, ServiceMetadata>();
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        ServiceMetadata serviceMetadata = ddClient.checkService(serviceName);
        deploymentHost2Service.put(deploymentHost, serviceMetadata);
      } catch (ServiceNotFoundException | ServiceNotRunnableExceptionThrift e) {
        logger.warn("Service {} on remote {} doesn't exit", serviceName, deploymentHost);
        ServiceMetadata serviceMetadata = new ServiceMetadata();
        serviceMetadata.setServiceName(serviceName);
        serviceMetadata.setServiceStatus(ServiceStatus.UNKNOWN);
        deploymentHost2Service.put(deploymentHost, serviceMetadata);
      } catch (Exception e) {
        logger.error("Caught an exception when restart service {} on remote {}", serviceName,
            deploymentHost,
            e);
        return null;
      }
    }
    return deploymentHost2Service;
  }

  @Override
  public boolean transfer() {
    final DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    final List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    Path packagePathToDeploy = null;
    logger.info("service {}, packages' path: {}", serviceName, config.getPackagesPath());
    String[] packageNames = config.getPackagesPath().toFile().list();

    // get deploy packages for multi packages of coordinator/fsserver
    for (String packageName : packageNames) {
      logger.info("deal with package {}", packageName);
      if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
        if (packageName
            .contains(PyService.findValueByServiceName(serviceName).getServiceProjectKeyName())
            && packageName.contains(coordinatorTimestamp)
        ) {
          packagePathToDeploy = Paths.get(config.getPackagesPath().toString(), packageName);
          if (packagePathToDeploy.toFile().isDirectory()) {
            logger.warn("Found a directory1 {} of which name contains the package name!",
                packagePathToDeploy);
            continue;
          } else {
            break;
          }
        }
      }  else {
        if (packageName
            .contains(PyService.findValueByServiceName(serviceName).getServiceProjectKeyName())) {
          packagePathToDeploy = Paths.get(config.getPackagesPath().toString(), packageName);
          if (packagePathToDeploy.toFile().isDirectory()) {
            logger.warn("Found a directory {} of which name contains the package name!",
                packagePathToDeploy);
            continue;
          } else {
            break;
          }
        }
      }
    }
    if (packagePathToDeploy == null) {
      logger.error("Unable to find package for service {}", serviceName);
      return false;
    }

    int nhostsperthread;
    if (serviceDeploymentHosts.size() < config.getDeploymentThreadAmount()) {
      nhostsperthread = 1;
    } else {
      nhostsperthread = serviceDeploymentHosts.size() / config.getDeploymentThreadAmount();
    }

    final ByteBuffer packageBytes = loadPackageToMemory(packagePathToDeploy);
    packageBytes.clear();

    final CountDownLatch threadLatch = new CountDownLatch(config.getDeploymentThreadAmount());
    final AtomicBoolean failed = new AtomicBoolean(false);

    for (int threadIndex = 0; threadIndex < config.getDeploymentThreadAmount(); threadIndex++) {
      final int remaining = serviceDeploymentHosts.size() - threadIndex * nhostsperthread;
      if (remaining <= 0) {
        threadLatch.countDown();
        continue;
      }

      final int hostBeginIndex = threadIndex * nhostsperthread;
      final int hostEndnigIndex;
      if (threadIndex == config.getDeploymentThreadAmount() - 1) {
        // last thread
        hostEndnigIndex = hostBeginIndex + remaining;
      } else {
        hostEndnigIndex = hostBeginIndex + nhostsperthread;
      }

      Thread thread = new Thread() {
        public void run() {
          try {
            for (String deploymentHost : serviceDeploymentHosts
                .subList(hostBeginIndex, hostEndnigIndex)) {
              DeploymentDaemonClientWrapper ddClient = null;
              try {
                //  Coordinator use timestamp for driver upgrade
                if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
                  ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort(),
                      config.getOperationTimeoutMs());
                  logger.info("Transfer package of {} {} to host {} ...", serviceName,
                      coordinatorTimestamp, deploymentHost);
                  ddClient.transferPackage(serviceName, config.getPackageVersion(),
                      packageBytes.duplicate(), coordinatorTimestamp);
                  logger.info("Done transfer on host {}", deploymentHost);
                } else {
                  ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort(),
                      config.getOperationTimeoutMs());
                  logger.info("Transfer package of {} to host {} ...", serviceName, deploymentHost);
                  ddClient.transferPackage(serviceName, config.getPackageVersion(),
                      packageBytes.duplicate());
                  logger.info("Done transfer on host {}", deploymentHost);
                }
              } catch (Exception e) {
                logger.error("Caught an exception when transfer package of service {} to remote {}",
                    serviceName, deploymentHost, e);
                failed.set(true);
                addFailureHost(DeploymentOperation.TRANSFER, deploymentHost);
              }
            }
          } finally {
            threadLatch.countDown();
          }
        }
      };

      thread.start();
    }

    try {
      threadLatch.await();
    } catch (InterruptedException e) {
      logger.error("", e);
    } finally {
      if (failed.get()) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean prepareWorkspace() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);
    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());

        PrepareWorkspaceRequest request = new PrepareWorkspaceRequest(RequestIdBuilder.get(),
            config.getPackageVersion(), serviceName);
        // Coordinator use timestamp
        if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
          logger.debug("prepareWorkspace {} {}", serviceName, coordinatorTimestamp);
          request.setCoorTimestamp(coordinatorTimestamp);
        }

        ddClient.getClient().prepareWorkspace(request);
      } catch (Exception e) {
        logger.error("Caught an exception when prepare workspace of service {} on remote {}",
            serviceName,
            deploymentHost, e);
        return false;
      }
    }

    return true;
  }

  /*
   * get coordinator info and update latest version when dc deploy
   * drivercontainer need latest version to start coordinator so if only wipeout drivercontainer
   * and then deploy it we should update latest version according
   * coordinator deploy info and running packages exists or not
   *
   */

  public boolean updateLatestVersion() {
    boolean success1 = updateLatestVersionByService(PyService.COORDINATOR.getServiceName());
    return success1;
  }

  /**
   * xx.
   */
  public boolean updateLatestVersionByService(String serviceName) {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);
    logger.debug("updateLatestVersionByService {} {}", serviceName, config.getServiceTimestamp());
    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());

        UpdateLatestVersionRequest request = new UpdateLatestVersionRequest(RequestIdBuilder.get(),
            serviceName, config.getPackageVersion());
        if (serviceName.equals(PyService.COORDINATOR.getServiceName())) {
          request.setCoorTimestamp(config.getServiceTimestamp());
        }

        ddClient.getClient().updateLatestVersion(request);
      } catch (Exception e) {
        logger.error("Caught an exception when prepare workspace of service {} on remote {}",
            serviceName,
            deploymentHost, e);
        return false;
      }
    }
    return true;
  }
  
  /**
   * xx.
   */
  public boolean checkUpgradeStatus() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    List<String> pendingHosts = waitUntilUpgradeSuccess(serviceName, serviceDeploymentHosts,
        config.getDeploymentDaemonPort(), config.getOperationTimeoutMs());

    if (!pendingHosts.isEmpty()) {
      addFailureHosts(DeploymentOperation.UPGRADE, pendingHosts);
      return false;
    }
    return true;
  }

  // only support driver online upgrade
  List<String> waitUntilUpgradeSuccess(String serviceName, List<String> hostList,
      int ddPort, long timeout) {
    logger.info("Wait until service {} on {} Upgrade Success", serviceName, hostList);

    final long waitInternal = 5000;
    List<String> pendingHostList = new ArrayList<String>();
    List<String> travelHostList = new ArrayList<String>();
    for (String host : hostList) {
      travelHostList.add(host);
    }
    for (int tryCount = 0; tryCount < timeout / waitInternal; tryCount++) {
      pendingHostList.clear();
      Iterator<String> iter = travelHostList.iterator();
      while (iter.hasNext()) {
        String host = iter.next();
        try {
          DeploymentDaemonClientWrapper ddClient = ddClientFactory.build(host, ddPort);
          boolean upgrading = ddClient.checkUpgradeStatus(serviceName);
          logger.info("Service {} on {} is {}", serviceName, host,
              upgrading ? "Upgrading" : "Upgraded");
          if (upgrading) {
            pendingHostList.add(host);
          }
        } catch (Exception e) {
          logger.debug("Unable to check state of service {} on {}, due to {}", serviceName, host,
              e.toString());
          pendingHostList.add(host);
          continue;
        }
      }
      if (pendingHostList.isEmpty()) {
        return pendingHostList;
      }
      if (!travelHostList.isEmpty()) {
        logger.info(String.format("Wait for more %d seconds ...", waitInternal / 1000));
        try {
          Thread.sleep(waitInternal);
        } catch (InterruptedException e) {
          logger.info("Catch an Exception{}", e);
        }
      } else {
        logger.warn(" Service {} on  hostList {} is ERROR", serviceName, hostList);
        break;
      }
    }
    if (!travelHostList.isEmpty()) {
      logger.error("Operation timeout for {} seconds", timeout / 1000);
    }
    return pendingHostList;
  }

  List<String> waitUntilServiceOnAllHostsTurnInto(String serviceName, ServiceStatus status,
      List<String> hostList,
      int ddPort, long timeout) {
    if (PyService.findValueByServiceName(serviceName).getServiceLauchingScriptName().isEmpty()) {
      logger.warn("Service launcher for {} is not specified, maybe it is a driver!", serviceName);
      return new ArrayList<>();
    }

    logger.info("Wait until service {} on {} turns into {}", serviceName, hostList, status.name());

    final long waitInternal = 5000;

    List<String> pendingHostList = new ArrayList<String>();
    List<String> travelHostList = new ArrayList<String>();
    for (String host : hostList) {
      travelHostList.add(host);
    }
    for (int tryCount = 0; tryCount < timeout / waitInternal; tryCount++) {
      pendingHostList.clear();
      Iterator<String> iter = travelHostList.iterator();
      while (iter.hasNext()) {
        String host = iter.next();
        try {
          DeploymentDaemonClientWrapper ddClient = ddClientFactory.build(host, ddPort);
          ServiceMetadata service = ddClient.checkService(serviceName);

          logger
              .info("Service {} on {} is {}", serviceName, host, service.getServiceStatus().name());
          if (service.getServiceStatus().name().equals(ServiceStatus.ERROR.name())) {
            logger.error(service.getErrorCause());
            pendingHostList.add(host);
            iter.remove();
            continue;
          }

          if (service.getServiceStatus() != status) {
            pendingHostList.add(host);
          }
        } catch (Exception e) {
          logger.debug("Unable to check state of service {} on {}, due to {}", serviceName, host,
              e.toString());
          pendingHostList.add(host);
          continue;
        }
      }
      if (pendingHostList.isEmpty()) {
        return pendingHostList;
      }
      if (!travelHostList.isEmpty()) {
        logger.info(String.format("Wait for more %d seconds ...", waitInternal / 1000));
        try {
          Thread.sleep(waitInternal);
        } catch (InterruptedException e) {
          logger.info("Catch an Exception{}", e);
        }
      } else {
        logger.warn(" Service {} on  hostList {} is ERROR", serviceName, hostList);
        break;
      }
    }
    if (!travelHostList.isEmpty()) {
      logger.error("Operation timeout for {} seconds", timeout / 1000);
    }
    return pendingHostList;
  }

  private List<String> pickServiceHostByGroup(String serviceName) {
    return pickServiceHostByGroup(serviceName, this.operationList);
  }

  /**
   * xx.
   */
  public List<String> pickServiceHostByGroup(String serviceName,
      List<DeploymentOperation> operationList) {
    List<String> serviceHosts = getHostsAfterFilterFailure(serviceName, operationList);

    DeploymentConfiguration serviceDeploymentConfiguration = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);
    if (serviceDeploymentConfiguration.getGroup() == null) {
      return getHostsAfterFilterFailure(serviceName, operationList);
    }

    // check if the service is datanode or arbiter
    if (!serviceName.equals(PyService.DATANODE.getServiceName())) {
      return getHostsAfterFilterFailure(serviceName, operationList);
    }

    List<String> serviceHostsInGroup = new ArrayList<String>();
    if (serviceName.equals(PyService.DATANODE.getServiceName())) {
      DataNodeDeploymentConfiguration dataNodeDeploymentConfiguration =
          (DataNodeDeploymentConfiguration) serviceDeploymentConfiguration;
      if (dataNodeDeploymentConfiguration.isGroupEnabled()) {
        /*
         * If group assignment is configured,
         *  then fill hosts being configured first. Note that configuring hosts in
         * group may be different from assignment
         * dynamically by information-center if it is configured after
         * deploying first time.
         */
        serviceHostsInGroup = dataNodeDeploymentConfiguration
            .getDeploymentHosts(serviceDeploymentConfiguration.getGroup());
        return serviceHostsInGroup;
      }
    }

    DihDeploymentConfiguration dihDeploymentConfiguration =
        (DihDeploymentConfiguration) deploymentConfigurationFactory
            .getDeploymentConfiguration(PyService.DIH.getServiceName());

    for (String dihHost : dihDeploymentConfiguration.getServiceDeploymentHosts()) {
      int retryRemaining = 2;
      Set<Instance> instancesInGroup = null;
      DihServiceBlockingClientWrapper dihClient = null;

      while (retryRemaining-- >= 0) {
        try {
          dihClient = dihClientFactory
              .build(new EndPoint(dihHost, dihDeploymentConfiguration.getServicePort()));
        } catch (GenericThriftClientFactoryException e) {
          logger.warn(
              "Unable to build client to DIH service on [{}:{}],"
                  + " let's wait for 3 seconds and retry to connect it.",
              dihHost, dihDeploymentConfiguration.getServicePort(), e);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger.warn("Caught an exception", e1);
          }
          continue;
        } catch (Exception e) {
          logger
              .error("Caught an exception when build dih client to connect service listening on {}",
                  dihHost, dihDeploymentConfiguration.getServicePort(), e);
          retryRemaining = -1;
          continue;
        }

        try {
          instancesInGroup = dihClient
              .getInstanceInGroup(serviceDeploymentConfiguration.getGroup());
        } catch (TTransportException e) {
          logger.warn(
              "Something wrong on connection to DIH service on [{}:{}],"
                  + " let's wait for 3 seconds and retry to connect it.",
              dihHost, dihDeploymentConfiguration.getServicePort(), e);
          try {
            Thread.sleep(3000);
          } catch (InterruptedException e1) {
            logger.warn("Caught an exception", e1);
          }
          continue;
        } catch (Exception e) {
          logger.error("Caught an exception", e);
          retryRemaining = -1;
          continue;
        }

        break;
      }

      if (retryRemaining < 0) {
        logger.error(
            "Something wrong polling instances in group {} "
                + "from DIH service on [{}:{}], let's try next DIH.",
            serviceDeploymentConfiguration.getGroup(), dihHost,
            dihDeploymentConfiguration.getServicePort());
        continue;
      }

      if (instancesInGroup.isEmpty()) {
        logger.warn("No instance exists for service {} in group {}", serviceName,
            serviceDeploymentConfiguration.getGroup());
        break;
      }

      for (Instance instance : instancesInGroup) {
        String serviceHost = instance.getEndPoint().getHostName();
        if (serviceHosts.contains(serviceHost) && !serviceHostsInGroup.contains(serviceHost)) {
          serviceHostsInGroup.add(serviceHost);
        }
      }

      return serviceHostsInGroup;
    }

    logger.error("Unable to get full instance of service {} in the group {}", serviceName,
        serviceDeploymentConfiguration.getGroup());
    return null;
  }

  @Override
  public boolean backupKey() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneBackupKey = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        ddClient.backupKey(serviceName, config.getPackageVersion());
      } catch (Exception e) {
        logger.error("Caught an exception", e);
        doneBackupKey = false;
        addFailureHost(DeploymentOperation.BACKUP_KEY, deploymentHost);
        continue;
      }
    }
    return doneBackupKey;
  }

  @Override
  public boolean useBackupKey() {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    List<String> serviceDeploymentHosts = pickServiceHostByGroup(serviceName);
    if (serviceDeploymentHosts == null) {
      logger.error("Something wrong when pick hosts of service {} by group {}", serviceName,
          config.getGroup());
      return false;
    }

    boolean doneUseBackupKey = true;
    for (String deploymentHost : serviceDeploymentHosts) {
      DeploymentDaemonClientWrapper ddClient = null;
      try {
        ddClient = ddClientFactory.build(deploymentHost, config.getDeploymentDaemonPort());
        ddClient.useBackupKey(serviceName, config.getPackageVersion());
      } catch (Exception e) {
        logger.error("Caught an exception", e);
        doneUseBackupKey = false;
        addFailureHost(DeploymentOperation.USE_BACKUP_KEY, deploymentHost);
        continue;
      }
    }
    return doneUseBackupKey;
  }

  private ByteBuffer loadPackageToMemory(Path packagePath) {
    final int length = (int) packagePath.toFile().length();

    ByteBuffer buffer = ByteBuffer.allocate(length);

    FileChannel fileChannel;
    try {
      fileChannel = new FileInputStream(packagePath.toFile()).getChannel();
    } catch (FileNotFoundException e1) {
      logger.error("No such package {}", packagePath.toString());
      return null;
    }

    try {
      fileChannel.read(buffer);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      return null;
    } finally {
      try {
        if (null != fileChannel) {
          fileChannel.close();
        }
      } catch (IOException e) {
        logger.error("", e);
      }
    }

    return buffer;
  }

  private synchronized void addFailureHosts(DeploymentOperation operation, List<String> hosts) {
    List<String> failureHosts = failureTalbe.get(operation);
    if (null == failureHosts) {
      failureHosts = new ArrayList<String>();
      failureTalbe.put(operation, failureHosts);
    }

    failureHosts.addAll(hosts);
  }

  private synchronized void addFailureHost(DeploymentOperation operation, String host) {
    List<String> failureHosts = failureTalbe.get(operation);
    if (null == failureHosts) {
      failureHosts = new ArrayList<String>();
      failureTalbe.put(operation, failureHosts);
    }

    failureHosts.add(host);
  }

  private List<String> getHostsAfterFilterFailure(String serviceName,
      List<DeploymentOperation> operations) {
    DeploymentConfiguration config = deploymentConfigurationFactory
        .getDeploymentConfiguration(serviceName);

    Set<String> excludedHosts = new HashSet<String>();
    for (DeploymentOperation operation : operations) {
      List<String> failureHosts = failureTalbe.get(operation);
      if (null != failureHosts) {
        excludedHosts.addAll(failureHosts);
      }
    }

    List<String> serviceDeploymentHosts = new ArrayList<String>();
    for (String host : config.getServiceDeploymentHosts()) {
      if (excludedHosts.contains(host)) {
        continue;
      }
      serviceDeploymentHosts.add(host);
    }

    return serviceDeploymentHosts;
  }

  public Map<DeploymentOperation, List<String>> getFailures() {
    return failureTalbe;
  }

}
