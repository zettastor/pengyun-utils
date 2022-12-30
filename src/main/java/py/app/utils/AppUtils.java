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

package py.app.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.Utils;
import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.icshare.InstanceMetadata;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.membership.SegmentMembership;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.icshare.ListArchivesRequestThrift;
import py.thrift.icshare.ListArchivesResponseThrift;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.InstanceMetadataThrift;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class AppUtils {

  public static final int DIH_PORT = 10000;
  private static final Logger logger = LoggerFactory.getLogger(AppUtils.class);
  private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
  private static final int DEFAULT_CONNECTION_TIMEOUT_MS = 1000;
  private static final DihClientFactory dihClientFactory = new DihClientFactory(1,
      DEFAULT_CONNECTION_TIMEOUT_MS);
  private static final InformationCenterClientFactory informationCenterClientFactory =
      new InformationCenterClientFactory(
          1, DEFAULT_CONNECTION_TIMEOUT_MS);

  /**
   * xx.
   */
  public static Set<Instance> getAvailableInstances(Collection<String> dihHosts, String serviceName)
      throws Exception {
    for (String host : dihHosts) {
      try {
        return getAvailableInstances(host, serviceName);
      } catch (Exception e) {
        logger.debug("catch an exception", e);
      }
    }

    throw new Exception(
        "there is no available dih: " + dihHosts + ", to get service: " + serviceName);
  }

  public static Set<Instance> getAvailableInstances(String dihHost, String serviceName)
      throws Exception {
    return getInstances(dihHost, serviceName, true);
  }

  private static Set<Instance> getInstances(String dihHost, String serviceName, boolean careOnlyOk)
      throws Exception {
    Set<Instance> instances = new HashSet<>();
    for (Instance instance : getAllInstances(dihHost)) {
      if (instance.getName().equals(serviceName)) {
        if (careOnlyOk) {
          if (instance.getStatus() == InstanceStatus.HEALTHY) {
            instances.add(instance);
          }
        } else {
          instances.add(instance);
        }
      }
    }

    if (instances.isEmpty()) {
      throw new Exception(
          "there is no available service: " + serviceName + " in dih: " + dihHost + ", careOnlyOk: "
              + careOnlyOk);
    } else {
      logger.debug(
          "there is available service: " + serviceName + " in dih: " + dihHost + ", careOnlyOk: "
              + careOnlyOk);
      return instances;
    }
  }

  /**
   * xx.
   */
  public static Set<Instance> getAllInstances(String dihHost) throws Exception {
    DihServiceBlockingClientWrapper client = dihClientFactory
        .build(new EndPoint(dihHost, DIH_PORT), DEFAULT_REQUEST_TIMEOUT_MS);
    Set<Instance> instances = client.getInstanceAll();
    return instances;
  }

  /**
   * xx.
   */
  public static List<InstanceMetadata> getAllArchiveInDatanode(InformationCenter.Iface client)
      throws TException {
    List<InstanceMetadata> datanodeList = new ArrayList<InstanceMetadata>();
    ListArchivesRequestThrift request = new ListArchivesRequestThrift();
    request.setRequestId(RequestIdBuilder.get());

    ListArchivesResponseThrift response = client.listArchives(request);
    for (InstanceMetadataThrift datanodeThrift : response.getInstanceMetadata()) {
      datanodeList.add(RequestResponseHelper.buildInstanceFrom(datanodeThrift));
    }
    return datanodeList;
  }

  public static InformationCenter.Iface generateInfoCenterSyncClient(EndPoint endPoint)
      throws Exception {
    return informationCenterClientFactory.build(endPoint, DEFAULT_REQUEST_TIMEOUT_MS).getClient();
  }

  /**
   * xx.
   */
  public static InformationCenter.Iface generateInfoCenterSyncClient(Collection<String> dihHosts,
      int timeoutSecond)
      throws Exception {
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < timeoutSecond * 1000) {
      Instance instance = null;
      try {
        Set<Instance> instances = getAvailableInstances(dihHosts,
            PyService.INFOCENTER.getServiceName());
        instance = (Instance) instances.toArray()[0];
      } catch (Exception e) {
        logger.warn("catch an exception", e);
      }

      if (instance != null) {
        try {
          InformationCenter.Iface client = generateInfoCenterSyncClient(instance.getEndPoint());
          logger.debug("Got a sync INFOCENTER client on {} cost {} millis",
              instance.getEndPoint().getHostName(), System.currentTimeMillis() - startTime);
          return client;
        } catch (Exception e) {
          logger.warn("caught an exception", e);
        }
      }
      Thread.sleep(2000);
    }

    throw new Exception(
        "can not get an available infocenter from dihs: " + dihHosts + ", timeout: " + timeoutSecond
            + " to generate sync client");
  }

  /**
   * xx.
   */
  public static InformationCenterClientWrapper generateInfoCenterClientWrapper(
      Collection<String> dihHosts, int timeoutSecond)
      throws Exception {
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < timeoutSecond * 1000) {
      Instance instance = null;
      try {
        Set<Instance> instances = getAvailableInstances(dihHosts,
            PyService.INFOCENTER.getServiceName());
        instance = (Instance) instances.toArray()[0];
      } catch (Exception e) {
        logger.warn("catch an exception", e);
      }

      if (instance != null) {
        try {
          InformationCenterClientWrapper client = generateInfoCenterClientWrapper(
              instance.getEndPoint());
          logger.debug("Got a sync INFOCENTER client on {} cost {} millis",
              instance.getEndPoint().getHostName(), System.currentTimeMillis() - startTime);
          return client;
        } catch (Exception e) {
          logger.warn("caught an exception", e);
        }
      }
      Thread.sleep(2000);
    }

    throw new Exception(
        "can not get an available infocenter from dihs: " + dihHosts + ", timeout: " + timeoutSecond
            + " to generate sync client");
  }

  public static InformationCenterClientWrapper generateInfoCenterClientWrapper(EndPoint endPoint)
      throws Exception {
    return informationCenterClientFactory.build(endPoint, DEFAULT_REQUEST_TIMEOUT_MS);
  }

  /**
   * xx.
   */
  public static void waitForVolumeAvailable(long volumeId, InformationCenter.Iface infoCenterClient,
      long accountId,
      int timeoutSeconds) throws Exception {
    long startTime = System.currentTimeMillis();
    VolumeMetadata volumeMetadata = null;
    long timePassed = 0;
    while (timePassed < timeoutSeconds * 1000) {
      try {
        timePassed = System.currentTimeMillis() - startTime;
        long costTime = System.currentTimeMillis() - startTime;
        volumeMetadata = getVolume(infoCenterClient, accountId, volumeId);
        if (!volumeMetadata.isVolumeAvailable()) {
          logger.info("wait volume: {} to become available, cost time: {}, {}", volumeId, costTime,
              volumeMetadata);
          return;
        }

        logger.info("{} seconds passed, and volume is still {}, not stable", timePassed / 1000,
            volumeMetadata.getVolumeStatus());

        Thread.sleep(2000);
      } catch (Exception e) {
        Thread.sleep(3000);
        logger.warn("catch an exception", e);
      }

    }

    String errorMsg =
        "I have waited for " + timePassed / 1000 + " seconds, but volume is still not stable. "
            + volumeMetadata;
    throw new TimeoutException(errorMsg);
  }

  /**
   * xx.
   */
  public static void waitForVolumeStable(long volumeId, InformationCenter.Iface infoCenterClient,
      long accountId,
      int timeoutSeconds) throws Exception {
    Utils.waitUntilConditionMatches(timeoutSeconds, "volume stable", () -> {
      try {
        VolumeMetadata volume = getVolume(infoCenterClient, accountId, volumeId);
        logger.debug("volume status {}", volume.getVolumeStatus());
        return checkVolumeStable(volume);
      } catch (Exception e) {
        logger.warn("exception", e);
        return false;
      }
    });
  }

  /**
   * xx.
   */
  public static boolean checkVolumeStable(VolumeMetadata volumeMetadata) throws Exception {
    if (volumeMetadata == null || !volumeMetadata.isVolumeAvailable()) {
      logger.debug("Not stable, volume is not available but {}", volumeMetadata.getVolumeStatus());
      return false;
    }

    List<SegmentMetadata> segDatas = volumeMetadata.getSegments();
    if (null == segDatas) {
      logger.error("segments is null, {} ", volumeMetadata);
      return false;
    }
    if (volumeMetadata.isVolumeAvailable()) {
      if (segDatas.size() == 0) {
        logger
            .debug("Not stable : volume satus is OK, but segments size is zero {}", volumeMetadata);
      }
    }

    // check if segment count equal to volume size divided by segment size
    if (segDatas.size() != volumeMetadata.getVolumeSize() / volumeMetadata.getSegmentSize()) {
      logger.debug("Not stable : volume doesn't have enough segments yet");
      return false;
    }

    for (SegmentMetadata segmentMetadata : segDatas) {
      SegmentMembership highestMembership = getHighestMembership(segmentMetadata);
      if (highestMembership == null || highestMembership.size() < 3) {
        logger.debug("Not stable, membership is null or size less than 3 ");
        return false;
      }

      SegmentUnitMetadata segmentUnitMetadata = segmentMetadata
          .getSegmentUnitMetadata(highestMembership.getPrimary());
      if (segmentUnitMetadata == null) {
        logger.debug("Not stable, segment unit meta data is null");
        return false;
      }

      // check that primary is ok
      if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Primary) {
        logger
            .debug("Not stable, the primary we got from membership is not in status primary but {}",
                segmentUnitMetadata.getStatus());
        return false;
      }

      if (!highestMembership.getJoiningSecondaries().isEmpty()) {
        logger.debug("Not stable, there are still joining secondaries");
        return false;
      }

      if (!highestMembership.getInactiveSecondaries().isEmpty()) {
        logger.debug("Not stable, there are still inactive secondaries");
        return false;
      }

      for (InstanceId instanceId : highestMembership.getAllSecondaries()) {
        segmentUnitMetadata = segmentMetadata.getSegmentUnitMetadata(instanceId);
        if (segmentUnitMetadata == null) {
          logger.debug("Not stable, a secondary we got from membership is not present");
          return false;
        } else if (segmentUnitMetadata.getStatus() != SegmentUnitStatus.Secondary
            && segmentUnitMetadata.getStatus() != SegmentUnitStatus.Arbiter) {
          logger.debug(
              "Not stable, a secondary we got from membership is not secondary or arbiter but {}",
              segmentUnitMetadata.getStatus());
          return false;
        }
      }

      if (segmentMetadata.getSegmentUnits().size() != volumeMetadata.getVolumeType()
          .getNumMembers()) {
        logger.debug("Not stable, segment's segment unit count is not {} but {}",
            volumeMetadata.getVolumeType().getNumMembers(),
            segmentMetadata.getSegmentUnits().size());
        return false;
      }

      // check for segment unit has the full membership
      for (SegmentUnitMetadata segUnit : segmentMetadata.getSegmentUnits()) {
        if (segUnit.getMembership().size() != volumeMetadata.getVolumeType().getNumMembers()) {
          logger.debug("Not stable, segment unit membership not full");
          return false;
        }
      }
    }

    logger.debug("Volume {} is stable now", volumeMetadata.getName());
    return true;
  }

  /**
   * xx.
   */
  public static VolumeMetadata getVolume(InformationCenter.Iface client, long accountId,
      long volumeId)
      throws Exception {
    GetVolumeRequest getVolumeRequest = new GetVolumeRequest();
    getVolumeRequest.setRequestId(RequestIdBuilder.get());
    getVolumeRequest.setAccountId(accountId);
    getVolumeRequest.setVolumeId(volumeId);
    GetVolumeResponse response = null;
    Exception exception = null;
    int retryTimes = 3;
    for (int retried = 0; retried < retryTimes; retried++) {
      try {
        response = client.getVolume(getVolumeRequest);
        break;
      } catch (Exception e) {
        logger.warn("failed to get volume", e);
        exception = e;
      }
    }
    if (null == response || !response.isSetVolumeMetadata()) {
      throw exception;
    } else {
      return RequestResponseHelper.buildVolumeFrom(response.getVolumeMetadata());
    }
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
