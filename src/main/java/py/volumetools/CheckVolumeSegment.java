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

package py.volumetools;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.aspectj.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * xx.
 */
public class CheckVolumeSegment {

  private static Logger logger = LoggerFactory.getLogger(CheckVolumeSegment.class);
  private static long accountId = SUPERADMIN_ACCOUNT_ID;
  private int localDihPort = 10000;
  private String hostName = "";

  public static void log4jInit() {
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    log4jInit();

    // initialize the parameter passed in.
    CommandLineArgs cliArgs = new CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);
    if (cliArgs.help) {
      commander.setProgramName("java -jar CheckVolumeSegment.jar");
      commander.usage();
      return;
    }

    String infoCenterAddress = cliArgs.infoCenterAddress;
    long volumeId = cliArgs.volumeId;
    String filePath = cliArgs.filePath;
    int infoCenterPort = cliArgs.infoCenterPort;
    if (infoCenterPort == 0) {
      infoCenterPort = 8020;
    }
    boolean autoCreate = cliArgs.autoCreate;

    CheckVolumeSegment checkInstance = new CheckVolumeSegment();
    CreateUnit createUnit = new CreateUnit();
    checkInstance.hostName = infoCenterAddress;

    InstanceStore instanceStore = checkInstance.instanceStore();
    try {
      EndPoint endPoint = new EndPoint(infoCenterAddress, infoCenterPort);
      InformationCenterClientWrapper informationCenterClientWrapper = checkInstance
          .buildInfoCenterClientWrapper(endPoint);

      VolumeMetadata volume = informationCenterClientWrapper.getVolume(volumeId, accountId);

      Set<Integer> missIndexSet = new HashSet<>();
      //check
      Map<Integer, SegmentMetadata> onlyOneUnitSegmentMap = checkInstance
          .checkVolume(volume, filePath, missIndexSet);

      if (autoCreate) {
        //create segment unit if segment have only one unit
        logger.warn(
            "**************************** process only have one unit segment *******************");
        for (SegmentMetadata segmentMetadata : onlyOneUnitSegmentMap.values()) {
          // filter the segment unit who not in membership
          SegmentUnitMetadata segmentUnitMetadata = null;
          for (SegmentUnitMetadata segmentUnitMetadataTemp : segmentMetadata.getSegmentUnits()) {
            Set<InstanceId> members = segmentUnitMetadataTemp.getMembership().getMembers();
            if (members.contains(segmentUnitMetadataTemp.getInstanceId())) {
              segmentUnitMetadata = segmentUnitMetadataTemp;
              break;
            }
          }
          if (segmentUnitMetadata == null) {
            logger.warn("found none segment unit to create unit!!!!!!!! the segment is:{}",
                segmentMetadata);
            continue;
          }

          Set<InstanceId> members = segmentUnitMetadata.getMembership().getMembers();
          members.remove(segmentUnitMetadata.getInstanceId());

          InstanceId instanceId = members.iterator().next();

          EndPoint dataNodeEndPoint = instanceStore.get(instanceId).getEndPoint();

          int segmentIndex = segmentMetadata.getIndex();
          try {
            logger
                .warn(
                    "--------- in volume {}, in index -- :" 
                        + "{}, in data node :{}, begin to create unit",
                    volumeId,
                    segmentIndex, dataNodeEndPoint);

            createUnit.createUnit(volume, segmentIndex, 1, dataNodeEndPoint,
                segmentMetadata.getLatestMembership(), 0);

            Thread.sleep(3000);
          } catch (Exception e) {
            logger.warn("create unit for volume:{} in index :{} ,data node :{}, find some error:",
                volumeId, segmentIndex, dataNodeEndPoint, e);
          }

        } //create only one

        //fix missing segment
        logger.warn(
            "**************************** process missing segment ****************************");
        SegmentMembership exampleMembership = volume.getSegments().get(0).getLatestMembership();
        Set<InstanceId> members = exampleMembership.getMembers();
        for (int segIndex : missIndexSet) {
          logger.warn("try to create unit on missing seg index:{}. membership:{}", segIndex,
              exampleMembership);
          int createCount = 0;
          for (InstanceId instanceId : members) {
            EndPoint dataNodeEndPoint = instanceStore.get(instanceId).getEndPoint();

            try {
              logger.warn(
                  "--------- in volume {}, in index -- :{}, in data node :{}, begin to create unit",
                  volumeId, segIndex, dataNodeEndPoint);
              createUnit.createUnit(volume, segIndex, 1, dataNodeEndPoint,
                  exampleMembership, 0);
              Thread.sleep(3000);
            } catch (Exception e) {
              logger.warn("create unit for volume:{} in index :{} ,data node :{}, find some error:",
                  volumeId, segIndex, dataNodeEndPoint, e);
            }

            createCount++;
            if (createCount >= volume.getVolumeType().getVotingQuorumSize()) {
              break;
            }
          }
          logger.warn("create {} unit on seg index:{} over", createCount, segIndex);
        }
      } //if (autoCreate) {
    } catch (Exception e) {
      logger.error("Caught an exception when get volume info", e);
    }
    System.exit(0);
  }

  public EndPoint localDihEp() {
    return EndPointParser.parseLocalEndPoint(localDihPort, hostName);
  }

  public DihClientFactory dihClientFactory() {
    DihClientFactory dihClientFactory = new DihClientFactory(1);
    return dihClientFactory;
  }

  /**
   * xx.
   */
  public InstanceStore instanceStore() throws Exception {
    Object instanceStore = DihInstanceStore.getSingleton();
    ((DihInstanceStore) instanceStore).setDihClientFactory(dihClientFactory());
    ((DihInstanceStore) instanceStore).setDihEndPoint(localDihEp());
    ((DihInstanceStore) instanceStore).init();
    Thread.sleep(5000);
    return (InstanceStore) instanceStore;
  }

  /**
   * xx.
   */
  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory informationCenterClientFactory =
        new InformationCenterClientFactory();
    informationCenterClientFactory.setInstanceName(PyService.INFOCENTER.getServiceName());
    informationCenterClientFactory.setInstanceStore(instanceStore());
    return informationCenterClientFactory;
  }

  public InformationCenterClientWrapper buildInfoCenterClientWrapper(EndPoint endPoint)
      throws Exception {
    InformationCenterClientFactory infoCenterClientFactory = informationCenterClientFactory();
    return infoCenterClientFactory.build(endPoint, 6000);
  }

  /**
   * xx.
   */
  public Map<Integer, SegmentMetadata> checkVolume(VolumeMetadata volume, String filePath,
      Set<Integer> missIndexSet) {
    Map<Integer, SegmentMetadata> badSegmentMetadataMap = new HashMap<>();
    Map<Integer, SegmentMetadata> onlyOneSegmentMetadataMap = new HashMap<>();
    Map<Integer, SegmentMetadata> goodSegmentMetadataMap = new HashMap<>();
    VolumeType volumeType = volume.getVolumeType();

    int segCount = volume.getSegmentCount();
    Set<Integer> segIndexSet = volume.getSegmentTable().keySet();
    if (missIndexSet == null) {
      missIndexSet = new HashSet<>();
    }
    if (segIndexSet.size() != segCount) {
      for (int i = 0; i < segCount; i++) {
        if (!segIndexSet.contains(i)) {
          missIndexSet.add(i);
        }
      }
    }

    for (SegmentMetadata segmentMetadata : volume.getSegments()) {
      SegmentMembership membership = segmentMetadata.getLatestMembership();
      List<SegmentUnitMetadata> unitMetadataList = segmentMetadata.getSegmentUnits();

      Map<InstanceId, SegmentUnitMetadata> segmentUnitMetadataMap = unitMetadataList.stream()
          .collect(
              Collectors.toMap(SegmentUnitMetadata::getInstanceId, v -> v));

      InstanceId primary = membership.getPrimary();
      Set<InstanceId> secondaries = membership.getSecondaries();
      Set<InstanceId> jsSet = membership.getJoiningSecondaries();

      // 1. check whether join secondary instance id same as primary or secondary
      Set<InstanceId> jsSetBac = new HashSet<>(jsSet);
      jsSetBac.retainAll(secondaries);
      if (jsSet.contains(primary) || !jsSetBac.isEmpty()) {
        badSegmentMetadataMap.put(segmentMetadata.getIndex(), segmentMetadata);
        continue;
      }

      // 2. check status and quorum
      int availableCount = 0;
      if ((segmentUnitMetadataMap.get(primary) != null)
          && !segmentUnitMetadataMap.get(primary).getStatus().hasGone()) {
        availableCount++;
      }
      for (InstanceId secondary : secondaries) {
        if ((segmentUnitMetadataMap.get(secondary) != null)
            && !segmentUnitMetadataMap.get(secondary).getStatus().hasGone()) {
          availableCount++;
        }
      }
      for (InstanceId joinSecondary : jsSet) {
        if ((segmentUnitMetadataMap.get(joinSecondary) != null)
            && !segmentUnitMetadataMap.get(joinSecondary).getStatus().hasGone()) {
          availableCount++;
        }
      }
      if (availableCount == 1) {
        onlyOneSegmentMetadataMap.put(segmentMetadata.getIndex(), segmentMetadata);
        continue;
      }

      goodSegmentMetadataMap.put(segmentMetadata.getIndex(), segmentMetadata);
    }
    String badSegContent = "";
    badSegContent += "\r\n---------------------------miss segment------------\r\n";
    badSegContent += "found " + missIndexSet.size() + " segment was missed! missed seg index: \r\n";
    badSegContent += missIndexSet;
    badSegContent += "\r\n---------------------------only one segment---------\r\n";
    badSegContent +=
        "found " + onlyOneSegmentMetadataMap.size() + " segment has only one unit: \r\n";
    badSegContent += onlyOneSegmentMetadataMap.toString();
    badSegContent += "\r\n---------------------------bad segment-------------\r\n";
    badSegContent += "found " + badSegmentMetadataMap.size() + " bad segment: \r\n";
    badSegContent += badSegmentMetadataMap.toString();

    String goodSegContent = "\r\n-----------------------------good segment------" 
        + "\r\n";
    goodSegContent += "found " + goodSegmentMetadataMap.size() + " good segment: \r\n";
    goodSegContent += goodSegmentMetadataMap.toString();

    if (filePath != null) {
      FileUtil.writeAsString(new File(filePath), badSegContent + goodSegContent);
    } else {
      logger.warn("{}", badSegContent);
      logger.info("{}", goodSegContent);
    }

    logger.warn(
        "found {} bad segment. {} segment only have one unit. {} segment missing. {} good segment",
        badSegmentMetadataMap.size(), onlyOneSegmentMetadataMap.size(), missIndexSet.size(),
        goodSegmentMetadataMap.size());

    return onlyOneSegmentMetadataMap;
  }

  // The command line parser
  private static class CommandLineArgs {

    public static final String INFO_CENTER_ADDRESS = "--infoCenterAddress";
    public static final String INFO_CENTER_PORT = "--infoCenterPort";
    public static final String VOLUME_ID = "--volumeId";
    public static final String RESULT_FILE = "--file";
    public static final String AUTO_CREATE = "--autoCreate";
    @Parameter(names = INFO_CENTER_ADDRESS,
        description = "the address of info center. like '10.0.2.101'", required = true)
    public String infoCenterAddress;
    @Parameter(names = INFO_CENTER_PORT,
        description = "the port of info center. default is 8020.", required = false)
    public int infoCenterPort;
    @Parameter(names = VOLUME_ID, description = "volume id", required = true)
    public long volumeId;
    @Parameter(names = RESULT_FILE, description = 
        "the full file path which will save check result", required = false)
    public String filePath;
    @Parameter(names = AUTO_CREATE,
        description = "whether auto create segment unit " 
            + "if found segment only have one unit or segment missing. default is true",
        required = false)
    public boolean autoCreate;

    @Parameter(names = "--help", help = true)
    private boolean help;
  }
}
