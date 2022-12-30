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

package py.utils.coordinator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.DeploymentDaemonClientFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Constants;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.lib.Coordinator;
import py.coordinator.volumeinfo.VolumeInfoRetrieverImpl;
import py.dd.DeploymentDaemonClientHandler;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.icshare.Domain;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.informationcenter.StoragePool;
import py.informationcenter.StoragePoolStrategy;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.icshare.CreateVolumeResponse;
import py.thrift.icshare.ListArchivesRequestThrift;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.share.ArchiveMetadataThrift;
import py.thrift.share.CacheTypeThrift;
import py.thrift.share.CreateDomainRequest;
import py.thrift.share.CreateStoragePoolRequestThrift;
import py.thrift.share.InstanceMetadataThrift;
import py.thrift.share.ListDomainRequest;
import py.thrift.share.ListDomainResponse;
import py.thrift.share.OneDomainDisplayThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNameExistedExceptionThrift;
import py.thrift.share.VolumeTypeThrift;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class VolumeUtils {

  private static final Logger logger = LoggerFactory.getLogger(VolumeUtils.class);
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 5000;
  private static final int DD_PORT = 10002;
  private static final int MAX_CHANNEL_PENDING_SIZE_MB = 200;
  private static long accountId = Constants.SUPERADMIN_ACCOUNT_ID;
  private static Set<Instance> dihs;
  private static EndPoint dihEndPoint;
  private static DihClientFactory dihFactory = new DihClientFactory(1,
      DEFAULT_CONNECTION_TIMEOUT_MS);
  private static InformationCenterClientFactory infoCenterFactory = 
      new InformationCenterClientFactory(
      1,
      DEFAULT_CONNECTION_TIMEOUT_MS);
  private static DeploymentDaemonClientFactory deploymentDaemonClientFactory =
      new DeploymentDaemonClientFactory(
      1);
  private static DeploymentDaemonClientHandler deploymentDaemonClientHandler = null;

  /**
   * xx.
   */
  public static void close() {
    try {
      infoCenterFactory.close();
    } catch (Exception e) {
      logger.error("fail to close info center factory", e);
    }

    try {
      dihFactory.close();
    } catch (Exception e) {
      logger.error("fail to close dih factory", e);
    }

    try {
      deploymentDaemonClientFactory.close();
    } catch (Exception e) {
      logger.error("fail to close dd factory", e);
    }

    try {
      DihInstanceStore.getSingleton().close();
    } catch (Exception e) {
      logger.error("fail to close dih store factory", e);
    }
  }

  public static Set<Instance> getServiceInstance(String serviceName)
      throws GenericThriftClientFactoryException, TException {
    return getServiceInstance(serviceName, InstanceStatus.HEALTHY);
  }

  /**
   * xx.
   */
  public static Set<Instance> getServiceInstance(String serviceName, InstanceStatus status)
      throws GenericThriftClientFactoryException, TException {
    Set<Instance> instances = new HashSet<Instance>();
    for (Instance instance : dihs) {
      try {
        DihServiceBlockingClientWrapper wrapper = dihFactory.build(instance.getEndPoint());
        return wrapper.getInstances(serviceName, status);
      } catch (Exception e) {
        logger.error("fail to get a OK dih, all dih={}", dihs, e);
      }
    }
    return instances;
  }

  /**
   * xx.
   */
  public static Coordinator getCoordinator(long volumeId) throws Exception {
    VolumeInfoRetrieverImpl volumeInfoRetriever = new VolumeInfoRetrieverImpl();
    volumeInfoRetriever.setInfoCenterClientFactory(infoCenterFactory);

    GenericThriftClientFactory<DataNodeService.AsyncIface> asyncFactory = GenericThriftClientFactory
        .create(DataNodeService.AsyncIface.class)
        .withMaxChannelPendingSizeMb(MAX_CHANNEL_PENDING_SIZE_MB);
    GenericThriftClientFactory<DataNodeService.Iface> syncFactory = GenericThriftClientFactory
        .create(DataNodeService.Iface.class)
        .withMaxChannelPendingSizeMb(MAX_CHANNEL_PENDING_SIZE_MB);

    Coordinator coordinator = new Coordinator(DihInstanceStore.getSingleton(), syncFactory,
        asyncFactory,
        null);

    InstanceId id = new InstanceId(RequestIdBuilder.get());
    logger.warn("coordinator id={}", id);
    coordinator.setDriverContainerId(id);

    logger.warn("successfully instantiated a coordinator");

    return coordinator;
  }

  /**
   * xx.
   */
  public static void setDihEndPoint(EndPoint dih) throws Exception {
    DihServiceBlockingClientWrapper wrapper = dihFactory.build(dih);
    dihs = wrapper.getInstances(PyService.DIH.getServiceName(), InstanceStatus.HEALTHY);
    dihEndPoint = dih;
    DihInstanceStore instanceStore = DihInstanceStore.getSingleton();
    instanceStore.setDihClientFactory(dihFactory);
    instanceStore.setDihEndPoint(dihEndPoint);
    instanceStore.init();
    infoCenterFactory.setInstanceStore(instanceStore);
  }

  /**
   * xx.
   */
  public static void waitForVolumeStable(long volumeId, long timeoutMs) throws Exception {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        InformationCenterClientWrapper infoWrapper = getInfoClient();
        if (infoWrapper == null) {
          logger.warn("InfoCenter Client is null, try to get it again");
          throw new Exception();
        }

        VolumeMetadata volumeMetadata = infoWrapper.getVolume(volumeId, accountId);
        if (volumeMetadata == null || !volumeMetadata.isVolumeAvailable()) {
          logger.warn("volume={} is not available", volumeMetadata);
          throw new Exception();
        }

        List<SegmentMetadata> segDatas = volumeMetadata.getSegments();
        if (volumeMetadata.isVolumeAvailable()) {
          if (segDatas.size() == 0) {
            logger
                .warn("volume satus is OK, but segments size is zero, metadata={}", volumeMetadata);
            throw new Exception();
          }
        }

        // check if segment count equal to volume size divided by segment size
        long segmentCount = volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize();
        if (segDatas.size() != segmentCount) {
          logger.warn(
              "current segment count is not same as volume size divided by" 
                  + " segment size, segment count={}, expect size is {}",
              segmentCount, volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize());
          throw new Exception();
        }

        for (SegmentMetadata segmentMetadata : segDatas) {
          SegmentMembership highestMembership = getHighestMembership(segmentMetadata);
          if (highestMembership == null || highestMembership.size() < 3) {
            logger.warn("semgent={}, membership={}", segmentMetadata.getIndex(), highestMembership);
            throw new Exception();
          }

          SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
              .getSegmentUnitMetadata(highestMembership.getPrimary());
          if (segmentUnitMetadata == null) {
            logger.warn("semgent={} there is no segment unit metedata for membership={}",
                segmentMetadata.getIndex(), highestMembership);
            throw new Exception();
          }

          // check that primary is ok
          if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Primary) {
            logger.warn("semgent={}, not primary for membership={}", segmentMetadata.getIndex(),
                highestMembership);
            throw new Exception();
          }

          for (InstanceId instanceId : highestMembership.getSecondaries()) {
            segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
            if (segmentUnitMetadata == null
                || segmentUnitMetadata.getStatus() != SegmentUnitStatus.Secondary) {
              logger.warn("semgent={} not secondary for segment unit metadata={}",
                  segmentMetadata.getIndex(), segmentUnitMetadata);
              throw new Exception();
            }
          }

          if (segmentMetadata.getSegmentUnits().size() != volumeMetadata.getVolumeType()
              .getNumMembers()) {
            logger.warn("semgent={}, segment unit metadata count={}", segmentMetadata.getIndex(),
                segmentMetadata.getSegmentUnits().size());
            throw new Exception();
          }

          // check for segment unit has the full membership
          for (SegmentUnitMetadata segUnit : segmentMetadata.getSegmentUnits()) {
            if (segUnit.getMembership().size() != volumeMetadata.getVolumeType().getNumMembers()) {
              logger.warn("semgent={}, membership count={}", segmentMetadata.getIndex(),
                  segUnit.getMembership().size());
              throw new Exception();
            }
          }
        }

        long costTime = System.currentTimeMillis() - startTime;
        logger.warn("@@ volume={} is stable, costTime={}", volumeMetadata, costTime);
        return;
      } catch (Exception e) {
        logger.warn("caught an exception", e);
        Thread.sleep(5000);
      }
    }

    throw new Exception(
        "can not wait for volume=" + volumeId + " become stable, cost time=" + timeoutMs);
  }

  /**
   * xx.
   */
  public static void waitForVolumeAvailable(long volumeId, long timeoutMs) throws Exception {
    long startTime = System.currentTimeMillis();
    VolumeMetadata metadata = null;
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        InformationCenterClientWrapper infoWrapper = getInfoClient();
        if (infoWrapper == null) {
          logger.warn("InfoCenter Client is null, try to get it again");
          throw new Exception();
        }

        metadata = infoWrapper.getVolume(volumeId, accountId);
        if (metadata != null && metadata.isVolumeAvailable()) {
          logger.warn("wait volume=" + metadata + " to become available, cost time: "
              + (System.currentTimeMillis() - startTime));
          return;
        } else {
          logger.warn("volume={} is not available", metadata);
          Thread.sleep(5000);
        }

      } catch (Exception e) {
        logger.warn("fail to wait volume={}  to become available", volumeId, e);
        Thread.sleep(5000);
      }
    }

    throw new Exception(
        "can not wait for volume=" + volumeId + " become available, cost time=" + timeoutMs);
  }

  /**
   * xx.
   */
  public static void waitForServiceToSpecifiedStatus(Instance instance, InstanceStatus status,
      long timeoutMs)
      throws Exception {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        Set<Instance> instances = getServiceInstance(instance.getName(), status);
        for (Instance tmp : instances) {
          if (tmp.getId().getId() == instance.getId().getId()) {
            logger.warn("wait for instance={} becoming status={}, cost time={}", instance, status,
                System.currentTimeMillis() - startTime);
            return;
          }
        }
      } catch (GenericThriftClientFactoryException | TException e1) {
        logger.warn("fail to get instance={}", instance, e1);
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
    }

    throw new Exception(
        "can not wait for instance=" + instance + " become " + status + ", cost time=" + timeoutMs);
  }

  /**
   * xx.
   */
  public static void deactive(Instance instance, long timeoutMs) {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        if (deploymentDaemonClientHandler == null) {
          deploymentDaemonClientHandler = new DeploymentDaemonClientHandler();
          deploymentDaemonClientHandler
              .setDeploymentDaemonClientFactory(deploymentDaemonClientFactory);
        }
        deploymentDaemonClientHandler.deactivate(instance.getEndPoint().getHostName(), DD_PORT,
            instance.getName(), instance.getEndPoint().getPort(), true);
        logger.warn("success to deactive instance={}", instance);
        break;
      } catch (Exception e) {
        logger.warn("fail to deactive datanode={}", instance, e);
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
    }
  }

  /**
   * xx.
   */
  public static void active(Instance instance, long timeoutMs) {
    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < timeoutMs) {
      try {
        if (deploymentDaemonClientHandler == null) {
          deploymentDaemonClientHandler = new DeploymentDaemonClientHandler();
          deploymentDaemonClientHandler
              .setDeploymentDaemonClientFactory(deploymentDaemonClientFactory);
        }

        deploymentDaemonClientHandler.activate(instance.getEndPoint().getHostName(), DD_PORT,
            instance.getName(), "");
        logger.warn("success to activev instance={}", instance);
        break;
      } catch (Exception e) {
        logger.warn("fail to deactive datanode={}", instance, e);
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
    }
  }




  /**
   * xx.
   */
  public static long createVolume(long volumeId, String volumeName, long volumeSize,
      String domainName,
      String storagePoolName) throws Exception {

    CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
    createVolumeRequest.setRequestId(RequestIdBuilder.get());
    createVolumeRequest.setAccountId(accountId);
    createVolumeRequest.setVolumeId(volumeId);
    createVolumeRequest.setName(volumeName);
    createVolumeRequest.setVolumeSize(volumeSize);
    createVolumeRequest.setVolumeType(VolumeTypeThrift.REGULAR);
    createVolumeRequest.setEnableLaunchMultiDrivers(true);

    InformationCenterClientWrapper infoWrapper = getInfoClient();

    ListDomainRequest listDomainRequest = new ListDomainRequest();
    listDomainRequest.setRequestId(RequestIdBuilder.get());
    listDomainRequest.setDomainIds(null);
    Domain domain = null;
    ListDomainResponse response = infoWrapper.getClient().listDomains(listDomainRequest);
    for (OneDomainDisplayThrift domainThrift : response.getDomainDisplays()) {
      if (domainThrift.getDomainThrift().getDomainName().equals(domainName)) {
        domain = RequestResponseHelper.buildDomainFrom(domainThrift.getDomainThrift());
        break;
      }
    }

    createVolumeRequest.setDomainId(domain.getDomainId());
    createVolumeRequest.setStoragePoolId((Long) domain.getStoragePools().toArray()[0]);
    logger.warn("create volume request={}", createVolumeRequest);
    CreateVolumeResponse createVolumeResponse = null;
    try {
      createVolumeResponse = infoWrapper.getClient().createVolume(createVolumeRequest);
    } catch (VolumeNameExistedExceptionThrift e) {
      logger.warn("volume={} is existing", volumeName);
      ListVolumesRequest listVolumesRequest = new ListVolumesRequest();
      listVolumesRequest.setAccountId(accountId);
      listVolumesRequest.setRequestId(RequestIdBuilder.get());
      ListVolumesResponse listVolumesResponse = infoWrapper.getClient()
          .listVolumes(listVolumesRequest);
      for (VolumeMetadataThrift metadata : listVolumesResponse.getVolumes()) {
        if (0 == metadata.getName().compareTo(volumeName)) {
          return metadata.getVolumeId();
        }
      }
    }

    if (createVolumeResponse == null || createVolumeResponse.getVolumeId() != createVolumeRequest
        .getVolumeId()) {
      String errMsg = "failed to create volume : " + createVolumeRequest;
      throw new Exception(errMsg);
    }

    return volumeId;
  }

  /**
   * xx.
   */
  public static void createDomainAndStoragePool(String domainName, String storagePoolName)
      throws GenericThriftClientFactoryException, TException {
    InformationCenterClientWrapper infoWrapper = getInfoClient();

    // check domain exist
    ListDomainRequest listDomainRequest = new ListDomainRequest();
    listDomainRequest.setRequestId(RequestIdBuilder.get());
    listDomainRequest.setDomainIds(null);
    ListDomainResponse response = infoWrapper.getClient().listDomains(listDomainRequest);
    for (OneDomainDisplayThrift domain : response.getDomainDisplays()) {
      if (domain.getDomainThrift().getDomainName().equals(domainName)) {
        return;
      }
    }

    // create default domain and default storage pool
    long domainId = RequestIdBuilder.get();
    final long storagePoolId = RequestIdBuilder.get();

    // get all datanodes
    Set<Long> datanodeIds = new HashSet<Long>();
    for (Instance instance : VolumeUtils.getServiceInstance(PyService.DATANODE.getServiceName())) {
      datanodeIds.add(instance.getId().getId());
    }

    String domainDescription = "domain for Copy page";
    Set<Long> storagePools = new HashSet<Long>();
    Domain domain = new Domain();
    domain.setDomainId(domainId);
    domain.setDomainName(domainName);
    domain.setDomainDescription(domainDescription);
    domain.setDataNodes(datanodeIds);
    domain.setStoragePools(storagePools);
    domain.setLastUpdateTime(System.currentTimeMillis());

    String poolDescription = "storage pool for copy page";
    Multimap<Long, Long> archivesInDataNode = Multimaps
        .synchronizedSetMultimap(HashMultimap.<Long, Long>create());
    StoragePool storagePool = new StoragePool();
    storagePool.setPoolId(storagePoolId);
    storagePool.setDomainId(domainId);
    storagePool.setName(storagePoolName);
    storagePool.setDescription(poolDescription);
    storagePool.setStrategy(StoragePoolStrategy.Capacity);
    storagePool.setArchivesInDataNode(archivesInDataNode);
    storagePool.setLastUpdateTime(System.currentTimeMillis());

    ListArchivesRequestThrift listArchivesRequest = new ListArchivesRequestThrift();
    List<InstanceMetadataThrift> datanodes = infoWrapper.getClient()
        .listArchives(listArchivesRequest)
        .getInstanceMetadata();
    for (InstanceMetadataThrift datanode : datanodes) {
      long datanodeId = datanode.getInstanceId();
      datanodeIds.add(datanodeId);
      for (ArchiveMetadataThrift archive : datanode.getArchiveMetadata()) {
        long archiveId = archive.getArchiveId();
        archivesInDataNode.put(datanodeId, archiveId);
      }
    }

    // create domain
    CreateDomainRequest createDomainRequest = new CreateDomainRequest();
    createDomainRequest.setRequestId(RequestIdBuilder.get());
    createDomainRequest.setDomain(RequestResponseHelper.buildDomainThriftFrom(domain));
    try {
      logger.warn("create domain request={}", createDomainRequest);
      infoWrapper.getClient().createDomain(createDomainRequest);
    } catch (Exception e) {
      logger.error("fail to create domain", createDomainRequest, e);
      throw new TException();
    }

    // create storage pool
    CreateStoragePoolRequestThrift createStoragePoolRequest = new CreateStoragePoolRequestThrift();
    createStoragePoolRequest.setRequestId(RequestIdBuilder.get());
    createStoragePoolRequest
        .setStoragePool(RequestResponseHelper.buildThriftStoragePoolFrom(storagePool, null));
    try {
      logger.warn("create storage pool request={}", createStoragePoolRequest);
      infoWrapper.getClient().createStoragePool(createStoragePoolRequest);
    } catch (Exception e) {
      logger.error("fail to create storage pool={}", createStoragePoolRequest, e);
      throw new TException();
    }
  }

  /**
   * xx.
   */
  public static InformationCenterClientWrapper getInfoClient()
      throws GenericThriftClientFactoryException, TException {
    Set<Instance> instances = getServiceInstance(PyService.INFOCENTER.getServiceName());
    InformationCenterClientWrapper infoWrapper = null;
    for (Instance instance : instances) {
      try {
        infoWrapper = infoCenterFactory.build(instance.getEndPoint());
        break;
      } catch (Exception e) {
        logger.warn("fail to build a client to instance={}", instance, e);
      }
    }

    return infoWrapper;
  }

  private static SegmentMembership getHighestMembership(SegmentMetadata segmentMetadata) {
    SegmentMembership highestMembership = null;
    for (Map.Entry<InstanceId, SegmentUnitMetadata> entry : segmentMetadata
        .getSegmentUnitMetadataTable()
        .entrySet()) {
      if (entry.getValue().getMembership().compareTo(highestMembership) > 0) {
        highestMembership = entry.getValue().getMembership();
      }
    }

    return highestMembership;
  }
}
