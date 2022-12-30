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

package py.debug.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.DeploymentDaemonClientFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.debug.DebuggerOutput;
import py.debug.DebuggerOutputImpl;
import py.debug.cmd.Cmd;
import py.debug.cmd.CmdManager;
import py.debug.cmd.ExitCmd;
import py.debug.cmd.HelpCmd;
import py.debug.cmd.SetLogLevelCmd;
import py.debug.cmd.ShowLogLevel;
import py.dih.client.DihClientFactory;
import py.drivercontainer.client.CoordinatorClientFactory;
import py.drivercontainer.client.DriverContainerClientFactory;
import py.exception.GenericThriftClientFactoryException;
import py.exception.InvalidFormatException;
import py.fsservice.client.FsServiceClientFactory;
import py.infocenter.client.InformationCenterClientFactory;
import py.monitorserver.client.MonitorServerClientFactory;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public abstract class AbstractServiceDebugger {

  public static final String LS = "ls";
  public static final String SET_METRIC_CONFIG = "setMetricConfig";
  public static final String SHOW_METRIC_CONFIG = "showMetricConfig";
  public static final String SET_LOG_LEVEL = "setLogLevel";
  public static final String SHOW_LOG_LEVEL = "showLogLevel";
  public static final String SHOW_CONFIG = "showConfig";
  public static final String SET_CONFIG = "setConfig";
  public static final String EXIT = "exit";
  public static final String SHOW_IO = "showIO";
  public static final String SWITCH_ENABLE_WRITE_MUTATIONS = "switchEnableWriteMutations";
  public static final String SHOW_SERVICE = "showService";
  public static final String SET_CONFIG_OF_DATANODE = "setConfigOfDataNode";
  public static final String GET_EPOCH = "getEpoch";
  private PyService serviceType;
  private EndPoint endPoint = null;
  private String hostName = null;
  private String port = null;
  private DebuggerOutput out = DebuggerOutputImpl.getInstance();

  public AbstractServiceDebugger(PyService pyService) {
    this.serviceType = pyService;
  }

  /**
   * xx.
   */
  public boolean init() {
    String hostAndPort = this.hostName + ":";

    if (this.port == null) {
      hostAndPort = hostAndPort + getDefaultPort(); //use default port
    } else {
      hostAndPort = hostAndPort + this.port; //use the port the user assign
    }

    try {
      endPoint = new EndPoint(hostAndPort);
    } catch (InvalidFormatException ex) {
      return false;
    }

    DebugConfigurator.Iface debugConfigurator = null;
    try {
      debugConfigurator = getDebugConfigurator();
    } catch (GenericThriftClientFactoryException e) {
      out.print("get client failed!\n");
      return false;
    }

    List<ImmutablePair<String, Cmd>> cmdList = new ArrayList<>();
    addCmd(debugConfigurator, cmdList);
    CmdManager cmdManager = CmdManager.getInstance();
    cmdManager.registeCmd(serviceType, cmdList);
    cmdManager.setServiceType(serviceType);
    return true;
  }

  protected void addCmd(DebugConfigurator.Iface debugConfigurator,
      List<ImmutablePair<String, Cmd>> cmdList) {
    cmdList.add(new ImmutablePair<>(LS, new HelpCmd()));
    cmdList.add(new ImmutablePair<>(SET_LOG_LEVEL, new SetLogLevelCmd(debugConfigurator)));
    cmdList.add(new ImmutablePair<>(SHOW_LOG_LEVEL, new ShowLogLevel(debugConfigurator)));
    cmdList.add(new ImmutablePair<>(EXIT, new ExitCmd()));
  }

  private DebugConfigurator.Iface getDebugConfigurator()
      throws GenericThriftClientFactoryException {
    switch (serviceType) {
      case INFOCENTER:
        InformationCenterClientFactory informationCenterClientFactory =
            new InformationCenterClientFactory(
                1);
        return informationCenterClientFactory.build(endPoint).getClient();
      case DEPLOYMENTDAMON:
        DeploymentDaemonClientFactory deploymentDaemonClientFactory =
            new DeploymentDaemonClientFactory(
                1);
        return deploymentDaemonClientFactory.build(endPoint).getClient();
      case COORDINATOR:
        CoordinatorClientFactory coordinatorClientFactory = new CoordinatorClientFactory(1);
        return coordinatorClientFactory.build(endPoint).getClient();
      case DIH:
        DihClientFactory dihClientFactory = new DihClientFactory(1);
        return dihClientFactory.build(endPoint).getDelegate();
      case DRIVERCONTAINER:
        DriverContainerClientFactory driverContainerClientFactory =
            new DriverContainerClientFactory(
                1);
        return driverContainerClientFactory.build(endPoint).getClient();
      case DATANODE:
        GenericThriftClientFactory<DataNodeService.Iface> dataNodeServiceFactory =
            GenericThriftClientFactory
                .create(DataNodeService.Iface.class);
        return dataNodeServiceFactory.generateSyncClient(endPoint, 5000, 10000);
      case CONSOLE:
      default:
        throw new GenericThriftClientFactoryException();
    }
  }

  private int getDefaultPort() {
    switch (serviceType) {
      case DEPLOYMENTDAMON:
        return 10002;
      case DIH:
        return 10000;
      case DATANODE:
        return 10011;
      case DRIVERCONTAINER:
        return 9000;
      case COORDINATOR:
        return 2234;
      case INFOCENTER:
        return 8020;
      case CONSOLE:
      default:
        return 0;
    }
  }

  public String getPrefix() {
    return "[" + this.hostName + "-" + serviceType.getServiceName() + "]";
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public PyService getServiceType() {
    return serviceType;
  }
}
