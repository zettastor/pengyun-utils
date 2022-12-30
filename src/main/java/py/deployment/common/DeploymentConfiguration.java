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

import java.io.FileInputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import py.common.PyService;
import py.common.struct.EndPointParser;
import py.instance.Group;
import py.service.configuration.manager.ConfigurationStore;

/**
 * A class as configuration for deployment. When deploy all services to remote machines, some
 * configuration is required to specified such location of the machines, description of network,
 * path of services' packages and so on. The instance of the configuration is used through process
 * of deployment.
 */
@PropertySource("classpath:config/deploy.properties")
@Configuration
public class DeploymentConfiguration {

  protected static final String INTEG_TEST_XML_NAME = "integtest_settings.xml";
  protected static final String RELEASE_XML_NAME = "release_settings.xml";
  protected static final String MODULE_XML_NAME = "module_settings.xml";
  private static final Logger logger = LoggerFactory.getLogger(DeploymentConfiguration.class);
  protected Path rootPath = Paths.get(System.getProperty("user.dir"));
  protected Path packagesPath = Paths.get(rootPath.toString(), "packages");

  @Value("${thrift.transport.timeout:10000}")
  protected long thriftTransportTimeoutMs = 10000;
  @Value("${thrift.transport.maxsize:10000000}")
  protected long thriftTransportMaxSizeBytes = 10000000;
  @Value("${deployment.thread.amount:1}")
  protected int deploymentThreadAmount = 1;
  @Value("${daemon.port:10002}")
  protected int deploymentDaemonPort = 10002;
  @Value("${production.version}")
  protected String scene = DeploymentScene.INTERNAL.getValue();
  @Value("${remote.network}")
  protected String subNetwork = "10.0.1.0/24";

  /**
   * A list of params used for each operation. A common used param is "test", which specifies the
   * operation is processed in test environment.
   */
  protected List<String> paramList = new ArrayList<String>();

  /**
   * After deployment, each instance is assigned to a group. If the field is defined, each operation
   * is processed for each group. Otherwise, for each instance.
   */
  protected Group group;
  protected String serviceName;
  protected String serviceHostRange;
  protected int servicePort;
  protected int jmxAgentPort;
  protected long operationTimeoutMs;
  protected String serviceVersion;
  // used for coordinator & fsserver
  protected String serviceTimestamp;
  protected boolean groupEnabled;
  // used to control if the service need to deploy
  protected boolean deployEnabled;

  /**
   * A list of host to deploy service on. This field is parsed from the host range string.
   */
  protected List<String> serviceDeploymentHosts;

  protected Path xmlConfigurationPath;

  /**
   * Initialize deployment configuration.
   */
  public void initialize() {
    try {
      serviceDeploymentHosts = getAllHostsInRange(serviceHostRange);
    } catch (UnknownHostException e) {
      logger.error("Caught an exception", e);
      Validate.isTrue(false, "Wrong host range");
    }

    if (scene.equals(DeploymentScene.INTERNAL.getValue())) {
      xmlConfigurationPath = Paths.get(rootPath.toString(), "config", INTEG_TEST_XML_NAME);
    } else if (scene.equals(DeploymentScene.RELEASE.getValue())) {
      xmlConfigurationPath = Paths.get(rootPath.toString(), "config", RELEASE_XML_NAME);
    } else {
      xmlConfigurationPath = Paths.get(rootPath.toString(), "config", MODULE_XML_NAME);
    }
  }

  /**
   * Parse all hosts in the given host range string.
   *
   * <p>The format of host range string is like "10.0.1.1:10.0.1.10,10.0.1.23";
   * In the format, we use
   * ":" to represent a continuous range, and use "," to split discrete range.
   *
   */
  public List<String> getAllHostsInRange(String hostRange) throws UnknownHostException {
    if (hostRange == null || hostRange.isEmpty()) {
      return new ArrayList<String>();
    }

    List<String> serviceHostSubRanges = Arrays.asList(hostRange.split(","));
    List<String> serviceDeploymentHosts = new ArrayList<String>();

    for (String subRange : serviceHostSubRanges) {
      List<String> rangeEnds = Arrays.asList(subRange.split(":"));
      for (String end : rangeEnds) {
        if (!EndPointParser.isInSubnet(end, subNetwork)) {
          logger.error(
              "Unable to initialize deployment configuration," 
                  + " because service {} deployment host {} is not in subnet {}",
              serviceName, end, subNetwork);
          throw new RuntimeException("Unable to initialize deployment configuration");
        }
      }

      switch (rangeEnds.size()) {
        case 1:
          String serviceDeploymentHost = rangeEnds.get(0);
          serviceDeploymentHosts.add(serviceDeploymentHost);
          continue;
        case 2:
          String oneEnd = rangeEnds.get(0);
          String theOtherEnd = rangeEnds.get(1);

          long indexOfOneEnd = EndPointParser.getIp(oneEnd);
          long indexOfTheOtherEnd = EndPointParser.getIp(theOtherEnd);

          for (long i = Math.min(indexOfOneEnd, indexOfTheOtherEnd);
              i <= Math.max(indexOfTheOtherEnd,
                  indexOfOneEnd); i++) {
            serviceDeploymentHosts.add(EndPointParser.getIpInStr((int) i));
          }
          continue;
        default:
          serviceDeploymentHosts.clear();
          logger.error("Invalid format of service deployment range {}", serviceHostRange);
          throw new RuntimeException("Unable to initialize deployment configuration");
      }
    }

    return serviceDeploymentHosts;
  }

  public long getThriftTransportTimeoutMs() {
    return thriftTransportTimeoutMs;
  }

  public void setThriftTransportTimeoutMs(long thriftTransportTimeoutMs) {
    this.thriftTransportTimeoutMs = thriftTransportTimeoutMs;
  }

  public long getThriftTransportMaxSizeBytes() {
    return thriftTransportMaxSizeBytes;
  }

  public void setThriftTransportMaxSizeBytes(long thriftTransportMaxSizeBytes) {
    this.thriftTransportMaxSizeBytes = thriftTransportMaxSizeBytes;
  }

  public int getDeploymentDaemonPort() {
    return deploymentDaemonPort;
  }

  public void setDeploymentDaemonPort(int deploymentDaemonPort) {
    this.deploymentDaemonPort = deploymentDaemonPort;
  }

  public String getScene() {
    return scene;
  }

  public void setScene(String scene) {
    this.scene = scene;
  }

  public String getSubNetwork() {
    return subNetwork;
  }

  public void setSubNetwork(String subNetwork) {
    this.subNetwork = subNetwork;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getServiceVersion() {
    return serviceVersion;
  }

  public void setServiceVersion(String serviceVersion) {
    this.serviceVersion = serviceVersion;
  }

  public String getServiceTimestamp() {
    return serviceTimestamp;
  }

  public void setServiceTimestamp(String serviceTimestamp) {
    this.serviceTimestamp = serviceTimestamp;
  }

  public String getServiceHostRange() {
    return serviceHostRange;
  }

  public void setServiceHostRange(String serviceHostRange) {
    this.serviceHostRange = serviceHostRange;
  }

  public List<String> getServiceDeploymentHosts() {
    return serviceDeploymentHosts;
  }

  public void setServiceDeploymentHosts(List<String> serviceDeploymentHosts) {
    this.serviceDeploymentHosts = serviceDeploymentHosts;
  }

  public long getOperationTimeoutMs() {
    return operationTimeoutMs;
  }

  public void setOperationTimeoutMs(long operationTimeoutMs) {
    this.operationTimeoutMs = operationTimeoutMs;
  }

  public int getServicePort() {
    return servicePort;
  }

  public void setServicePort(int servicePort) {
    this.servicePort = servicePort;
  }

  public int getJmxAgentPort() {
    return jmxAgentPort;
  }

  public void setJmxAgentPort(int jmxAgentPort) {
    this.jmxAgentPort = jmxAgentPort;
  }

  public Path getRootPath() {
    return rootPath;
  }

  public void setRootPath(Path rootPath) {
    this.rootPath = rootPath;
  }

  public Path getPackagesPath() {
    return packagesPath;
  }

  public void setPackagesPath(Path packagesPath) {
    this.packagesPath = packagesPath;
  }

  public List<String> getParamList() {
    return paramList;
  }

  public void setParamList(List<String> paramList) {
    this.paramList = paramList;
  }

  public void addParam(String param) {
    this.paramList.add(param);
  }

  /**
   * The version in package name is composed by service version and deployment scene.
   *
   */
  public String getPackageVersion() {
    return String.format("%s-%s", serviceVersion.trim(), scene.trim());
  }

  public Group getGroup() {
    return group;
  }

  public void setGroup(Group group) {
    this.group = group;
  }

  public Path getXmlConfigurationPath() {
    return xmlConfigurationPath;
  }

  public void setXmlConfigurationPath(Path xmlConfigurationPath) {
    this.xmlConfigurationPath = xmlConfigurationPath;
  }

  public int getDeploymentThreadAmount() {
    return deploymentThreadAmount;
  }

  public void setDeploymentThreadAmount(int deploymentThreadAmount) {
    this.deploymentThreadAmount = deploymentThreadAmount;
  }

  public boolean isDeployEnabled() {

    return deployEnabled;
  }

  /**
   * xx.
   */
  @Configuration
  public static class DihDeploymentConfiguration extends DeploymentConfiguration {

    /**
     * It is necessary to launch center dih firstly and then launch other dihes when deploy dih.
     * Therefore, we need to know the hosts of center dih.
     */
    @Value("${DIH.center.host.list}")
    private String centerDihHostRange;

    private List<String> centerDihHosts;

    public DihDeploymentConfiguration() {
      this.serviceName = PyService.DIH.getServiceName();
    }

    /**
     * xx.
     */
    public void initialize() {
      super.initialize();
      try {
        centerDihHosts = getAllHostsInRange(centerDihHostRange);
        logger.info("checkXMLCenterDih centerDIHHosts {}", centerDihHosts);
        boolean centerdihfit = true;
        logger.info("centerDIH is always agreed {} for dos", centerdihfit);
        if (!centerdihfit) {
          String message = "Deploy center dih config not agreed with XML center dih config";
          logger.error(message);
          throw new IllegalArgumentException(message);
        }
      } catch (UnknownHostException e) {
        logger.error("Caught an exception", e);
        Validate.isTrue(false, "Wrong host range");
      }
    }

    /**
     * xx.
     */
    public boolean checkXmlCenterDih() {
      try {
        List<String> centerdihhostsxml = getCenterDihHostsFromXml();
        logger
            .info("checkXMLCenterDih centerDIHHosts {} xml {}", centerDihHosts, centerdihhostsxml);
        if (centerdihhostsxml != null) {
          if (centerDihHosts.size() != centerdihhostsxml.size()) {
            return false;
          }
          Collections.sort(centerDihHosts);
          Collections.sort(centerdihhostsxml);
          for (int i = 0; i < centerDihHosts.size(); i++) {
            if (!centerDihHosts.get(i).equals(centerdihhostsxml.get(i))) {
              return false;
            }
          }
        }
      } catch (Exception e) {
        logger.warn("checkXMLCenterDih exception {}, please check config!", e);
      }
      return true;
    }

    /**
     * xx.
     */
    public List<String> getCenterDihHostsFromXml() {
      try {
        // XMLConfigurationFileReader configurationStore = 
        // new XMLConfigurationFileReader(xmlConfigurationPath);
        ConfigurationStore configurationStore = ConfigurationStore.create()
            .from(xmlConfigurationPath.toAbsolutePath().toString()).build();
        logger.info("ConfigurationStore  {}", xmlConfigurationPath.toAbsolutePath().toString());
        try {
          configurationStore.load();
        } catch (Exception e) {
          logger.error("Caught an exception", e);
          return null;
        }

        Map<String, Properties> propsFileName2Properties = configurationStore
            .getProperties(PyService.DIH.getServiceProjectKeyName());
        Properties properties = propsFileName2Properties.get("instancehub.properties");
        String xmlcenterdihhostrange = null;
        List<String> xmlcenterdihhosts = new ArrayList<>();

        for (Object key : properties.keySet()) {
          if (((String) key).equals("center.dih.endpoint")) {
            xmlcenterdihhostrange = properties.getProperty((String) key);
          }
        }

        String[] endpoints = xmlcenterdihhostrange.split(",");

        // get ip
        for (int i = 0; i < endpoints.length; i++) {
          String[] endpoint = endpoints[i].split(":");
          xmlcenterdihhosts.add(endpoint[0]);
        }

        return xmlcenterdihhosts;

      } catch (Exception e) {
        logger.error("Caught an exception", e);
        return null;
      }
    }

    @Value("${DIH.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${DIH.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${DIH.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${DIH.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${DIH.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${DIH.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

    public List<String> getCenterDihHosts() {
      return centerDihHosts;
    }

    public void setCenterDihHosts(List<String> centerDihHosts) {
      this.centerDihHosts = centerDihHosts;
    }

    public String getCenterDihHostRange() {
      return centerDihHostRange;
    }

    public void setCenterDihHostRange(String centerDihHostRange) {
      this.centerDihHostRange = centerDihHostRange;
    }
  }

  /**
   * xx.
   */
  @Configuration
  public static class InfoCenterDeploymentConfiguration extends DeploymentConfiguration {

    public InfoCenterDeploymentConfiguration() {
      this.serviceName = PyService.INFOCENTER.getServiceName();
    }

    @Value("${InfoCenter.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${InfoCenter.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${InfoCenter.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${InfoCenter.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${InfoCenter.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${InfoCenter.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }

  /**
   * xx.
   */
  @Configuration
  public static class DriverContainerDeploymentConfiguration extends DeploymentConfiguration {

    public DriverContainerDeploymentConfiguration() {
      this.serviceName = PyService.DRIVERCONTAINER.getServiceName();
    }

    @Value("${DriverContainer.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${DriverContainer.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${DriverContainer.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${DriverContainer.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${DriverContainer.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${DriverContainer.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }

  /**
   * xx.
   */
  @Configuration
  public static class DdDeploymentConfiguration extends DeploymentConfiguration {

    public DdDeploymentConfiguration() {
      this.serviceName = PyService.DEPLOYMENTDAMON.getServiceName();
    }

    @Value("${deployment_daemon.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${deployment_daemon.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${deployment_daemon.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${Deployment_daemon.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${deployment_daemon.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${deployment_daemon.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }


  /**
   * xx.
   */
  @Configuration
  public static class DataNodeDeploymentConfiguration extends DeploymentConfiguration {

    public static final String DATANODE_GROUP_REGEX = 
        "DataNode\\.deployment\\.host\\.group\\.(\\d+)";

    public static final Pattern DATANODE_GROUP_PATTERN = Pattern.compile(DATANODE_GROUP_REGEX);

    protected Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
    protected Map<Integer, List<String>> mapReturn = new HashMap<Integer, List<String>>();

    public DataNodeDeploymentConfiguration() {
      this.serviceName = PyService.DATANODE.getServiceName();
    }

    public boolean isGroupEnabled() {

      return groupEnabled;
    }

    @Value("${DataNode.deployment.host.group.enabled:false}")
    public void setGroupEnabled(boolean groupEnabled) {
      this.groupEnabled = groupEnabled;
    }

    /**
     * Get IP list according to groupId for each Group from the deploy.properties.
     *
     */

    public List<String> getDeploymentHosts(Group groupId) {
      mapReturn = this.parseGroupDeploymentHosts();
      return mapReturn.get(groupId.getGroupId());

    }

    /**
     * Get Hosts for each Group from deploy.properties to a map
     */

    public Map<Integer, List<String>> parseGroupDeploymentHosts() {
      Properties props = new Properties();
      try {
        FileInputStream in = new FileInputStream("config/deploy.properties");
        props.load(in);
      } catch (IOException e) {
        logger.error("Caught an exception", e);
        throw new IllegalStateException(e);
      }

      Enumeration<?> en = props.propertyNames();

      while (en.hasMoreElements()) {
        String key = (String) en.nextElement();
        String value = props.getProperty(key);
        int groupNum = parseGroupNum(key);

        if (groupNum >= 0) {
          List<String> hosts;

          try {
            hosts = getAllHostsInRange(value);
          } catch (UnknownHostException e) {
            logger.error("Caught an exception when parsing property [{}={}]", key, value, e);
            throw new IllegalArgumentException(e);
          }

          map.put(groupNum, hosts);
        }
      }

      if (map.size() >= 3) {
        return map;
      } else {
        logger.error("The group number can not be less than 3");
        throw new IllegalArgumentException();
      }
    }

    @Value("${DataNode.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${DataNode.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${DataNode.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${DataNode.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${DataNode.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    /**
     * Parse group number from the given key.
     *
     *
     * @return non-negative number if parsed a group number or -1.
     */
    public int parseGroupNum(String propertyKey) {
      Matcher groupMatcher;

      groupMatcher = DATANODE_GROUP_PATTERN.matcher(propertyKey);
      if (groupMatcher.find()) {
        return Integer.valueOf(groupMatcher.group(1));
      } else {
        return -1;
      }
    }

    @Value("${DataNode.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }

  /**
   * xx.
   */
  @Configuration
  public static class ConsoleDeploymentConfiguration extends DeploymentConfiguration {

    public ConsoleDeploymentConfiguration() {
      this.serviceName = PyService.CONSOLE.getServiceName();
    }

    @Value("${Console.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${Console.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${Console.deploy.port}")
    public void setServicePort(int servicePort) {
      this.servicePort = servicePort;
    }

    @Value("${Console.deploy.agent.jmx.port}")
    public void setJmxAgentPort(int jmxAgentPort) {
      this.jmxAgentPort = jmxAgentPort;
    }

    @Value("${Console.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${Console.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }

  /**
   * xx.
   */
  @Configuration
  public static class CoordinatorDeploymentConfiguration extends DeploymentConfiguration {

    public CoordinatorDeploymentConfiguration() {
      this.serviceName = PyService.COORDINATOR.getServiceName();
    }

    @Value("${Coordinator.version}")
    public void setServiceVersion(String serviceVersion) {
      this.serviceVersion = serviceVersion;
    }

    @Value("${Coordinator.timestamp}")
    public void setServiceTimestamp(String serviceTimestamp) {
      this.serviceTimestamp = serviceTimestamp;
    }

    @Value("${Coordinator.deploy.host.list}")
    public void setServiceHostRange(String serviceHostRange) {
      this.serviceHostRange = serviceHostRange;
    }

    @Value("${Coordinator.remote.timeout}")
    public void setOperationTimeoutMs(long operationTimeoutMs) {
      this.operationTimeoutMs = operationTimeoutMs;
    }

    @Value("${Coordinator.deploy:true}")
    public void setDeployEnabled(boolean deployEnabled) {
      this.deployEnabled = deployEnabled;
    }

  }
}
