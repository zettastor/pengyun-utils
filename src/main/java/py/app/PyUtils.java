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

package py.app;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;
import static py.common.struct.EndPointParser.IPV4_PATTERN;

import com.google.common.collect.Lists;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import py.app.utils.AppUtils;
import py.archive.RawArchiveMetadata;
import py.archive.StorageType;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.struct.EndPointParser;
import py.driver.DriverMetadata;
import py.driver.DriverStatus;
import py.icshare.InstanceMetadata;
import py.infocenter.client.InformationCenterClientWrapper;
import py.infocenter.client.VolumeMetadataAndDrivers;
import py.instance.Instance;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.CreateDomainRequest;
import py.thrift.share.CreateStoragePoolRequestThrift;
import py.thrift.share.DomainThrift;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.ListStoragePoolRequestThrift;
import py.thrift.share.ListStoragePoolResponseThrift;
import py.thrift.share.OneDomainDisplayThrift;
import py.thrift.share.StatusThrift;
import py.thrift.share.StoragePoolStrategyThrift;
import py.thrift.share.StoragePoolThrift;
import py.thrift.share.UpdateDomainRequest;
import py.thrift.share.UpdateStoragePoolRequestThrift;
import py.thrift.share.VolumeTypeThrift;

/**
 * xx.
 */
public class PyUtils {

  private static final long SUPER_ADMIN_ACCOUNT = SUPERADMIN_ACCOUNT_ID;
  private static final Pattern addressPattern = Pattern.compile(IPV4_PATTERN);
  private static Logger logger = LoggerFactory.getLogger(PyUtils.class);
  private static List<String> hosts;
  private static InformationCenter.Iface infoCenterClient;
  private static InformationCenterClientWrapper wrapper;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    String type = args[0];
    String[] subArgs = new String[args.length - 1];
    System.arraycopy(args, 1, subArgs, 0, args.length - 1);

    PropertySource propertySource = new SimpleCommandLinePropertySource(subArgs);
    AnnotationConfigApplicationContext commandLineContext =
        new AnnotationConfigApplicationContext();
    commandLineContext.getEnvironment().getPropertySources().addFirst(propertySource);

    if (type.equalsIgnoreCase("create_volume")) {
      commandLineContext.register(Config.class);
      commandLineContext.refresh();
      Config config = commandLineContext.getBean(Config.class);
      initLogger(config.debug);
      createEveryThing(config);
    }
  }

  private static void createEveryThing(Config config) throws Exception {

    logger.info("configurations {}", config);
    if (config.dih == null || config.dih.equals("")) {
      config.dih = EndPointParser
          .parseLocalEndPoint(AppUtils.DIH_PORT, InetAddress.getLocalHost().getHostAddress())
          .getHostName();
    }
    init(config.dih);

    // init
    AtomicReference<Set<Instance>> datanodesContainer = new AtomicReference<>();
    Utils.waitUntilConditionMatches(600, "data nodes ready", () -> {
      try {
        datanodesContainer
            .set(AppUtils.getAvailableInstances(hosts, PyService.DATANODE.getServiceName()));
        return datanodesContainer.get() != null
            && datanodesContainer.get().size() == config.availableDataNodes;
      } catch (Exception e) {
        return false;
      }
    });

    Utils.waitUntilConditionMatches(300, "archives ready", () -> {
      try {
        List<InstanceMetadata> instanceMetadataList = AppUtils
            .getAllArchiveInDatanode(infoCenterClient);
        logger.debug("instance {}", instanceMetadataList);
        return instanceMetadataList.size() == config.availableDataNodes && instanceMetadataList
            .stream()
            .allMatch(i -> i
                .getArchives()
                .size()
                > config.minArchiveCount);
      } catch (Exception e) {
        return false;
      }
    });

    // update domain
    Set<Instance> datanodes = AppUtils
        .getAvailableInstances(hosts, PyService.DATANODE.getServiceName());
    ListDomainResponse domains = infoCenterClient
        .listDomains(new ListDomainRequest(1L, SUPER_ADMIN_ACCOUNT));

    if (domains.getDomainDisplays() == null || domains.getDomainDisplays().isEmpty()) {
      logger.info("creating domain...");
      infoCenterClient.createDomain(new CreateDomainRequest(1L, SUPER_ADMIN_ACCOUNT,
          new DomainThrift(1L, "domain", "domain", new HashSet<>(), new HashSet<>(),
              StatusThrift.Available,
              System.currentTimeMillis(), 0L, 0L)));
      domains = infoCenterClient.listDomains(new ListDomainRequest(1L, SUPER_ADMIN_ACCOUNT));
    } else {
      logger.info("domain '{}' already exists",
          domains.getDomainDisplays().get(0).getDomainThrift().getDomainName());
    }

    OneDomainDisplayThrift domain = domains.getDomainDisplays().get(0);
    UpdateDomainRequest updateDomainRequest = new UpdateDomainRequest();
    DomainThrift newDomain = domain.getDomainThrift();
    newDomain
        .setDatanodes(datanodes.stream().map(d -> d.getId().getId()).collect(Collectors.toSet()));
    updateDomainRequest.setDomain(newDomain);
    updateDomainRequest.setRequestId(1L);
    updateDomainRequest.setAccountId(SUPER_ADMIN_ACCOUNT);
    infoCenterClient.updateDomain(updateDomainRequest);

    // update storage pool
    ListStoragePoolRequestThrift listStoragePoolRequest = new ListStoragePoolRequestThrift(1L,
        SUPER_ADMIN_ACCOUNT);
    listStoragePoolRequest.setDomainId(newDomain.getDomainId());
    ListStoragePoolResponseThrift listStoragePoolResponse = infoCenterClient
        .listStoragePools(listStoragePoolRequest);
    if (listStoragePoolResponse.getStoragePoolDisplays() == null || listStoragePoolResponse
        .getStoragePoolDisplays()
        .isEmpty()) {
      logger.info("creating storage pool...");
      infoCenterClient.createStoragePool(buildCreateStoragePoolRequest(newDomain.getDomainId()));
      listStoragePoolResponse = infoCenterClient.listStoragePools(listStoragePoolRequest);
    } else {
      logger.info("storage pool '{}' already exists",
          listStoragePoolResponse.getStoragePoolDisplays().get(0).getStoragePoolThrift()
              .getPoolName());
    }
    StoragePoolThrift storagePool = listStoragePoolResponse.getStoragePoolDisplays().get(0)
        .getStoragePoolThrift();
    List<InstanceMetadata> allDatanodes = AppUtils.getAllArchiveInDatanode(infoCenterClient);
    Map<Long, Set<Long>> archives = new HashMap<>();
    for (InstanceMetadata instance : allDatanodes) {
      logger.debug("instance {}", instance);
      archives.put(instance.getInstanceId().getId(),
          instance.getArchives().stream().filter(a -> !config.ssdOnly
              || a.getStorageType() == StorageType.SSD).map(RawArchiveMetadata::getArchiveId)
              .collect(Collectors.toSet()));
    }
    storagePool.setArchivesInDatanode(archives);
    infoCenterClient
        .updateStoragePool(
            new UpdateStoragePoolRequestThrift(1L, SUPER_ADMIN_ACCOUNT, storagePool));

    // create a volume
    if (config.volumeId == 0) {
      config.volumeId = System.currentTimeMillis();
    }
    logger.info("creating volume...");

    CreateVolumeRequest request = new CreateVolumeRequest(1L, config.volumeId, config.volumeId,
        String.valueOf(config.volumeId),
        config.volumeSizeMb * 1024 * 1024, 1024 * 1024, VolumeTypeThrift.SMALL,
        SUPER_ADMIN_ACCOUNT, "CREATE_VOLUME", new ArrayList<>(), newDomain.getDomainId(),
        storagePool.getPoolId(),
        true);
    infoCenterClient.createVolume(request);
    if (config.waitForVolumeStable) {
      AppUtils.waitForVolumeStable(config.volumeId, infoCenterClient, SUPER_ADMIN_ACCOUNT, 600);
    } else {
      AppUtils.waitForVolumeAvailable(config.volumeId, infoCenterClient, SUPER_ADMIN_ACCOUNT, 600);
    }
    List<String> validDriverHosts = Arrays.stream(config.driverHosts)
        .filter(h -> addressPattern.matcher(h).matches())
        .collect(Collectors.toList());
    // launch driver
    LaunchDriverRequestThrift launchDriverRequest = new LaunchDriverRequestThrift(1L,
        "driver" + System.currentTimeMillis(), SUPER_ADMIN_ACCOUNT, config.volumeId, 0,
        DriverTypeThrift.NBD, 1);
    for (String host : validDriverHosts) {
      logger.info("launching driver on {}", host);
      launchDriverRequest.setHostName(host);
      infoCenterClient.launchDriver(launchDriverRequest);

    }

    if (!validDriverHosts.isEmpty()) {
      Utils.waitUntilConditionMatches(20, "driver launched", () -> {
        try {
          VolumeMetadataAndDrivers volumeMetadataAndDrivers = wrapper
              .getVolumeByPagination(config.volumeId, Constants.SUPERADMIN_ACCOUNT_ID);
          List<DriverMetadata> drivers = volumeMetadataAndDrivers.getDriverMetadatas();
          return (drivers != null) && (drivers.size() == validDriverHosts.size()) && drivers
              .stream()
              .allMatch(
                  d -> d.getDriverStatus()
                      == DriverStatus.LAUNCHED);
        } catch (TException e) {
          logger.error("error", e);
          return false;
        } catch (Exception e) {
          logger.error("error", e);
          return false;
        }
      });
    }

    for (String host : validDriverHosts) {
      logger.info("trying to connect to server {} for {}", host, config.pydName);
      String cmd =
          "/opt/pyd/pyd-client " + config.volumeId + " 0 " + host + " /dev/" + config.pydName;
      String sshCmd = "ssh root@" + host + " " + cmd;
      Runtime.getRuntime().exec(sshCmd);
    }
    logger.info("done.");
  }

  private static CreateStoragePoolRequestThrift buildCreateStoragePoolRequest(long domainId) {
    CreateStoragePoolRequestThrift createStoragePoolRequest = new CreateStoragePoolRequestThrift();
    createStoragePoolRequest.setRequestId(RequestIdBuilder.get());
    createStoragePoolRequest.setAccountId(SUPER_ADMIN_ACCOUNT);
    StoragePoolThrift storagePoolThrift = new StoragePoolThrift();
    storagePoolThrift.setPoolId(2L);
    storagePoolThrift.setDomainId(domainId);
    storagePoolThrift.setPoolName("pool");
    storagePoolThrift.setDescription("pool");
    storagePoolThrift.setStrategy(StoragePoolStrategyThrift.Performance);
    createStoragePoolRequest.setStoragePool(storagePoolThrift);
    return createStoragePoolRequest;
  }

  protected static void init(String dihHost) throws Exception {
    hosts = Lists.newArrayList();
    logger.debug("looking for all the dih hosts...");
    Set<Instance> instances;
    try {
      instances = AppUtils.getAllInstances(dihHost);
    } catch (Exception e) {
      logger.error("can not get all instances", e);
      throw e;
    }
    for (Instance instance : instances) {
      String host = instance.getEndPoint().getHostName();
      if (!hosts.contains(host)) {
        hosts.add(host);
      }
    }
    logger.info("got dih hosts: {}", hosts);

    try {
      logger.debug("generate an infocenter client...");
      wrapper = AppUtils.generateInfoCenterClientWrapper(hosts, 30);
      infoCenterClient = AppUtils.generateInfoCenterSyncClient(hosts, 30);
    } catch (Exception e) {
      logger.warn("can not generate an infocenter client", e);
    }
  }

  private static void initLogger(boolean debug) {

    final Level level = debug ? Level.DEBUG : Level.INFO;
    PatternLayout layout = new PatternLayout();
    String conversionPattern = "%-5p[%d][%t]%C(%L):%m%n";
    // String conversionPattern = "[%d]%m%n";
    layout.setConversionPattern(conversionPattern);

    ConsoleAppender consoleAppender;
    // creates console appender
    consoleAppender = new ConsoleAppender();
    consoleAppender.setLayout(layout);
    consoleAppender.setThreshold(level);
    consoleAppender.setTarget("System.out");
    consoleAppender.setEncoding("UTF-8");
    consoleAppender.activateOptions();

    // creates file appender
    RollingFileAppender rollingFileAppender;
    rollingFileAppender = new RollingFileAppender();
    rollingFileAppender.setFile("py_utils_" + System.currentTimeMillis() + ".log");
    rollingFileAppender.setLayout(layout);
    rollingFileAppender.setThreshold(level);
    rollingFileAppender.setMaxBackupIndex(1);
    rollingFileAppender.setMaxFileSize("400MB");
    rollingFileAppender.activateOptions();

    // configures the root logger
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    rootLogger.setLevel(level);
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(consoleAppender);
    rootLogger.addAppender(rollingFileAppender);
  }
}
