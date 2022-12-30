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

package py.memory.nbd.server;


import static py.common.Utils.millsecondToString;
import static py.informationcenter.Utils.getByteSize;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.Validate;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.InstanceIdFileStore;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.coordinator.CoordinatorAppEngine;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.NbdServer;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.service.CoordinatorImpl;
import py.driver.DriverType;
import py.drivercontainer.driver.DriverAppContext;
import py.exception.StorageException;
import py.informationcenter.AccessPermissionType;
import py.instance.PortType;
import py.memorynbdserver.MemoryNbdServer;
import py.memorynbdserver.mocktools.MockCoordinatorBuilder;
import py.memorynbdserver.mocktools.MockExtendingVolumeUpdater;
import py.memorynbdserver.mocktools.MockIoLimitManager;
import py.memorynbdserver.mocktools.MockIoLimitScheduler;

/**
 * move from py.memoryNBDServer.MemoryNBDServerLauncherTest
 */
public class MemoryNbdServerLauncherTest {

  protected static Logger logger = LoggerFactory.getLogger(MemoryNbdServerLauncherTest.class);

  private static void usage() {
    String usageInfo =
        "usage: java -jar MemoryNBDServer.jar segmentCount "
            + "ipAddress port ioDepth [optional]accessAddress\n"
            + "param:segmentCount [1, 512](means use memory size:[1MB, 512MB])\n"
            + "param:ipAddress (like \"127.0.0.1\")\n" + "param:port (0-65535] will be fine\n"
            + "param:ioDepth(0-2048] will be accept, otherwise use 128 default\n"
            + "optional param:accessAddress (like \"127.0.0.1\") means"
            + " that you allow someone connect this server.\n"
            + "optional param:memory datanode ipAddress (like \"127.0.0.1\")\n"
            + "optional param:memory datanode port (0-65535] will be fine\n";
    logger.info(usageInfo);
  }

  @Test
  public void testNbdServer() {
    String[] args = new String[4];
    args[0] = "512";
    args[1] = "10.0.1.79";
    args[2] = "6666";
    args[3] = "128";

    if (args.length != 4 && args.length != 5 && args.length != 7) {
      usage();
      System.exit(-1);
    }

    boolean onlyMemory = false;
    if (args.length == 4) {
      onlyMemory = true;
    }

    int segmentCount = Integer.valueOf(args[0]);
    if (segmentCount <= 0) {
      logger.info("given segment count should be a positive number");
      System.exit(-1);
    }

    String coordinatorIp = null;
    try {
      coordinatorIp = args[1];
      Validate.isTrue(Utils.isIpAddress(coordinatorIp));
    } catch (Exception e) {
      logger.info("given ip address to listen should be a valid format");
      usage();
      System.exit(-1);
    }

    int coordinatorPort = (int) getByteSize(args[2]);
    if (coordinatorPort <= 0 || coordinatorPort > 65535) {
      logger.info("given port should between 0 to 65535");
      System.exit(-1);
    }

    int ioDepth = 128;
    int param = Integer.valueOf(args[3]);
    if (param > 0 && param <= 2048) {
      ioDepth = param;
    }

    logger.info("Memory NBD Server use IO DEPTH:[{}]", ioDepth);
    CoordinatorConfigSingleton.getInstance().setIoDepth(ioDepth);
    String startTimeStr = millsecondToString(System.currentTimeMillis());
    logger.info("Memory NBD Server will listen:[{}] [{}], and cost:[{}]MB memory, at:[{}]",
        coordinatorIp, coordinatorPort, segmentCount * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE,
        startTimeStr);

    boolean sendToDatanode = false;
    EndPoint datanodeEndPoint = null;
    if (args.length == 7) {
      String datanodeIp = null;
      try {
        datanodeIp = args[5];
        Validate.isTrue(Utils.isIpAddress(coordinatorIp));
      } catch (Exception e) {
        logger.info("given ip address to listen should be a valid format");
        usage();
        System.exit(-1);
      }

      int datanodePort = (int) getByteSize(args[6]);
      if (datanodePort <= 0 || datanodePort > 65535) {
        logger.info("given port should between 0 to 65535");
        System.exit(-1);
      }
      datanodeEndPoint = new EndPoint(datanodeIp, datanodePort);
      sendToDatanode = true;
    }

    MockCoordinatorBuilder mockCoordinatorBuilder = new MockCoordinatorBuilder(segmentCount,
        datanodeEndPoint);
    StorageDriver storageDriver = mockCoordinatorBuilder.buildMockCoordinator();
    MemoryNbdServer memoryNbdServer = null;
    NbdServer nbdServer = null;
    if (onlyMemory) {
      long memorySize = segmentCount * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE;
      memoryNbdServer = new MemoryNbdServer(memorySize, coordinatorIp, coordinatorPort, ioDepth,
          storageDriver);
    } else {
      String pydAccessIp = null;
      try {
        pydAccessIp = String.valueOf(args[4]);
        Validate.isTrue(Utils.isIpAddress(pydAccessIp));
      } catch (Exception e) {
        logger.info("given ip address for access should be a valid format");
        usage();
        System.exit(-1);
      }

      Map<String, AccessPermissionType> accessRuleMap = new ConcurrentHashMap<>();
      accessRuleMap.put(pydAccessIp, AccessPermissionType.READWRITE);

      try {
        /*new Coordinator,must init by open**/
        storageDriver.open(mockCoordinatorBuilder.getVolumeMetadata().getVolumeId(), 0);
      } catch (StorageException e) {
        logger.info("can not open storage driver");
        System.exit(-1);
      }
      NbdConfiguration nbdConfiguration = new NbdConfiguration();
      nbdConfiguration.setEndpoint(new EndPoint(coordinatorIp, coordinatorPort));
      nbdConfiguration.setDriverType(DriverType.NBD);
      nbdConfiguration.setVolumeId(mockCoordinatorBuilder.getVolumeMetadata().getVolumeId());
      nbdConfiguration.setVolumeAccessRuleTable(accessRuleMap);
      PydClientManager pydClientManager = new PydClientManager(0, true, 30, storageDriver);
      nbdServer = new NbdServer(nbdConfiguration, storageDriver, pydClientManager);

      MockIoLimitManager mockIoLimitManager = new MockIoLimitManager();
      MockIoLimitScheduler mockIoLimitScheduler = new MockIoLimitScheduler(mockIoLimitManager);
      nbdServer.setIoLimitScheduler(mockIoLimitScheduler);

      String appName = CoordinatorConfigSingleton.getInstance().getAppName();
      DriverAppContext appContext = new DriverAppContext(appName);
      // control stream
      EndPoint endpointOfControlStream = new EndPoint(coordinatorIp, coordinatorPort + 1);
      appContext.putEndPoint(PortType.CONTROL, endpointOfControlStream);
      // monitor stream
      EndPoint endpointOfMonitorStream = new EndPoint(coordinatorIp, coordinatorPort + 2);
      appContext.putEndPoint(PortType.MONITOR, endpointOfMonitorStream);

      appContext.setInstanceIdStore(
          new InstanceIdFileStore(appName, appName, endpointOfControlStream.getPort()));

      MockExtendingVolumeUpdater mockExtendingVolumeUpdater = new MockExtendingVolumeUpdater(null,
          null);
      CoordinatorImpl coordinatorImpl = CoordinatorImpl.getInstance();
      coordinatorImpl.setExtendingVolumeUpdater(mockExtendingVolumeUpdater);
      coordinatorImpl.setIoLimitScheduler(mockIoLimitScheduler);

      // start coordinator impl for metric purpose
      CoordinatorAppEngine coordinatorAppEngine = new CoordinatorAppEngine(coordinatorImpl);
      coordinatorAppEngine.setContext(appContext);
      try {
        coordinatorAppEngine.start();
      } catch (Exception e) {
        logger.info("failed to start coordinator engine");
        System.exit(-1);
      }
    }

    try {
      if (onlyMemory) {
        Validate.notNull(memoryNbdServer);
        memoryNbdServer.start();
      } else {
        Validate.notNull(nbdServer);
        nbdServer.start();
      }
      while (true) {
        Long currentTimeMs = System.currentTimeMillis();
        String currentTimeStr = millsecondToString(currentTimeMs);
        logger.info(
            "I am still alive now:[{}], contain coordinator"
                + " logic process:[{}], send to datanode:[{}]",
            currentTimeStr, !onlyMemory, sendToDatanode);
        Thread.sleep(5000);
      }
    } catch (Exception e) {
      System.exit(-1);
    }
  }
}
