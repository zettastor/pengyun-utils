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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.aspectj.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentMetadata.SegmentStatus;
import py.archive.segment.SegmentUnitMetadata;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.dih.client.DihClientFactory;
import py.dih.client.DihInstanceStore;
import py.infocenter.client.InformationCenterClientFactory;
import py.infocenter.client.InformationCenterClientWrapper;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.InstanceStore;
import py.membership.SegmentMembership;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.share.VolumeMetadataThrift;
import py.utils.test.FileOperationUtils;
import py.volume.VolumeMetadata;
import py.volume.VolumeStatus;
import py.volume.VolumeType;

/**
 * xx.
 */
public class CheckVolumeSegmentUnit {

  static File fileDirName = null;
  static boolean isFileSuccess;
  private static Logger logger = LoggerFactory.getLogger(CheckVolumeSegmentUnit.class);
  private static long accountId = SUPERADMIN_ACCOUNT_ID;
  //default current directory
  private static String localFileDir = System.getProperty("user.dir");
  private int localDihPort = 10000;
  private String hostName = "";
  private String checkPath;

  private String repairPath;

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
      commander.setProgramName("java -jar CheckVolumeSegmentUnit.jar");
      commander.usage();
      return;
    }
    String infoCenterAddress = cliArgs.infoCenterAddress;
    long volumeId = cliArgs.volumeId;
    int infoCenterPort = cliArgs.infoCenterPort;
    if (infoCenterPort == 0) {
      infoCenterPort = 8020;
    }
    CheckVolumeSegmentUnit checkInstance = new CheckVolumeSegmentUnit();
    checkInstance.hostName = infoCenterAddress;

    InstanceStore instanceStore = checkInstance.instanceStore();
    //get infoCenter host Address
    infoCenterAddress = checkInstance
        .getAcquisitionAndInputEndpoint(instanceStore, infoCenterAddress);

    try {
      EndPoint endPoint = new EndPoint(infoCenterAddress, infoCenterPort);
      InformationCenterClientWrapper informationCenterClientWrapper = checkInstance
          .buildInfoCenterClientWrapper(endPoint);
      String fileNameError = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
      checkInstance.checkPath = "check_" + (volumeId == 0 ? "" : volumeId + "_") + fileNameError;
      checkInstance.repairPath = "repair_" + (volumeId == 0 ? "" : volumeId + "_") + fileNameError;
      String checkFilePath = localFileDir + "/" + checkInstance.checkPath;
      //create log file
      isFileSuccess = createLocalTempFile(checkInstance.repairPath);
      VolumeMetadata volume = null;
      //volumeId if 0
      if (volumeId != 0) {
        volume = informationCenterClientWrapper.getVolume(volumeId, accountId);
        logger.warn("the volume data is:{}", new ObjectMapper().writeValueAsString(volume));
        checkInstance.repairSegmentUnit(instanceStore, volume, checkFilePath);
      } else {

        List<VolumeMetadata> metadataList = checkInstance.getListVolumes(checkInstance, accountId);
        logger.warn("the listVolume data:{}", new ObjectMapper().writeValueAsString(metadataList));
        for (VolumeMetadata volumeMetadata : metadataList) {
          //the volume status is Unavailable,go repair
          if (volumeMetadata.getVolumeStatus().equals(VolumeStatus.Unavailable)) {
            //get one volume
            volume = informationCenterClientWrapper
                .getVolume(volumeMetadata.getVolumeId(), accountId);
            checkInstance.repairSegmentUnit(instanceStore, volume, checkFilePath);
            logger.warn("ergodic the volumeName:{}, volumeId :{} is Unavailable",
                volumeMetadata.getName(), volumeMetadata.getVolumeId());
          }
        }

      }
    } catch (Exception e) {
      logger.error("Caught an exception when get volume info:", e);
      logPrintoutFile("****** Caught an exception when get volume info *****", null,
          "Caught an exception when get volume info:" + e);
    }
    System.exit(0);
  }

  /**
   * create local file or dir.
   */

  private static boolean createLocalTempFile(String fileName) {
    try {
      fileDirName = new File(localFileDir, fileName);
      return FileOperationUtils.createFile(fileDirName);
    } catch (IOException e) {
      logger.error("create file or write content exception:{}", e.getMessage());
      return false;
    }
  }

  private static void writerCon(StringBuilder con) throws IOException {
    FileWriter fw;
    //If the second parameter of this constructor is true, it is the additional content
    fw = new FileWriter(fileDirName.getPath(), true);
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(con.toString());
    bw.write("\n");
    bw.close();
    fw.close();
  }

  /**
   * log printout write file.
   */
  private static void logPrintoutFile(String title, String des, String content) {
    try {
      if (isFileSuccess) {
        StringBuilder contentBuilder = new StringBuilder();
        if (!StringUtils.isEmpty(title)) {
          contentBuilder.append(title).append("\n");
        }
        if (!StringUtils.isEmpty(des)) {
          contentBuilder.append(des).append("\n");
        }
        if (!StringUtils.isEmpty(content)) {
          contentBuilder.append(content);
        }
        writerCon(contentBuilder);
      }
    } catch (IOException e) {
      logger.error("write file error:{}", e.getMessage());
    }
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
   * get list volumes controller.
   */
  public List<VolumeMetadata> getListVolumes(CheckVolumeSegmentUnit checkInstance, long accountId) {
    try {
      ListVolumesRequest listVolumesFromInfoCenterRequest = new ListVolumesRequest();
      listVolumesFromInfoCenterRequest.setRequestId(RequestIdBuilder.get());
      listVolumesFromInfoCenterRequest.setAccountId(accountId);
      ListVolumesResponse listVolumeFromInfoCenterResponse = checkInstance
          .informationCenterClientFactory().build().getClient()
          .listVolumes(listVolumesFromInfoCenterRequest);
      List<VolumeMetadata> volumeMetadataList = new ArrayList<>();
      for (VolumeMetadataThrift volumeThrift : listVolumeFromInfoCenterResponse.getVolumes()) {
        VolumeMetadata volume = RequestResponseHelper.buildVolumeFrom(volumeThrift);
        volumeMetadataList.add(volume);
      }

      return volumeMetadataList;
    } catch (Exception e) {
      logger.error("Caught an exception when get controller listVolumes :", e);
    }
    return null;
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
    String badSegContent =
        "volume: " + volume.getVolumeId() + ",volumeName:" + volume.getName() + "  begin ";
    badSegContent += "\r\n---------------------------miss segment------------------\r\n";
    badSegContent += "found " + missIndexSet.size() + " segment was missed! missed seg index: \r\n";
    badSegContent += missIndexSet;
    badSegContent += "\r\n---------------------------only one segment--------------" 
        + "\r\n";
    badSegContent +=
        "found " + onlyOneSegmentMetadataMap.size() + " segment has only one unit: \r\n";
    badSegContent += "\r\n---------------------------bad segment--------------------" 
        + "\r\n";
    badSegContent += "found " + badSegmentMetadataMap.size() + " bad segment: \r\n";

    String goodSegContent = "\r\n-----------------------------good segment------------" 
        + "\r\n";
    goodSegContent += "found " + goodSegmentMetadataMap.size() + " good segment: \r\n";

    if (filePath != null) {
      FileUtil.writeAsString(new File(filePath), badSegContent + goodSegContent);
    }
    logger.warn("{}", badSegContent);
    logger.info("{}", goodSegContent);
    logPrintoutFile(null, null, "\n\n\n" + badSegContent + goodSegContent);
    logger.warn(
        "found {} bad segment. {} segment only have one unit. {} segment missing. {} good segment",
        badSegmentMetadataMap.size(), onlyOneSegmentMetadataMap.size(), missIndexSet.size(),
        goodSegmentMetadataMap.size());

    return onlyOneSegmentMetadataMap;
  }

  /**
   * repair Segment Unit.
   *
   */
  private void repairSegmentUnit(InstanceStore instanceStore, VolumeMetadata volume,
      String filePath) throws JsonProcessingException {
    CreateUnit createUnit = new CreateUnit();
    Set<Integer> missIndexSet = new HashSet<>();
    //check
    Map<Integer, SegmentMetadata> onlyOneUnitSegmentMap = checkVolume(volume, filePath,
        missIndexSet);
    long volumeId = volume.getVolumeId();

    //create segment unit if segment have only one unit
    if (onlyOneUnitSegmentMap.values().size() > 0) {
      logger.warn("***************** process only have one unit segment ****************");
      logPrintoutFile("************* process only have one unit segment **************",
          null, null);
    }
    boolean isOnlyOneCreateFirst = true;
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
      InstanceId aliveInstanceId = segmentUnitMetadata.getInstanceId();
      SegmentMembership membership = segmentUnitMetadata.getMembership();
      //Normal instance
      boolean isNormalPrimary = membership.isPrimary(aliveInstanceId);
      boolean isNormalSecondaries = membership.isAliveSecondaries(aliveInstanceId);
      Set<InstanceId> members = membership.getMembers();
      logger.warn("the Membership is:{}", members);
      if (members == null) {
        return;
      }
      //remove normal instanceId
      members.remove(aliveInstanceId);
      InstanceId instanceId = null;
      //ergodic bad instanceId，get instanceId is Primary or secondaries
      for (InstanceId member : members) {
        boolean isBadPrimary = membership.isPrimary(member);
        boolean isBadSecondaries = membership.isAliveSecondaries(member);
        //if normal instanceId is Primary ,so bad instanceId is Secondaries ,otherwise
        if ((isNormalPrimary && isBadSecondaries) || (isNormalSecondaries && isBadPrimary)) {
          instanceId = member;
        }
      }
      logger.warn("get instanceId is:{}", instanceId);
      EndPoint dataNodeEndPoint = instanceStore.get(instanceId).getEndPoint();
      int segmentIndex = segmentMetadata.getIndex();
      String title = null;
      try {
        logger.warn(
            "index -- :{}, in data node :{}, begin to create unit", segmentIndex,
            dataNodeEndPoint);
        logPrintoutFile("-index :" + segmentIndex + "," + "begin to create unit", null,
            null);
        createUnit.createUnit(volume, segmentIndex, 1, dataNodeEndPoint,
            segmentMetadata.getLatestMembership(), 0);

        if (isOnlyOneCreateFirst) {
          title = "------ create only have one unit for volume :" + volume.getName()
              + "(" + volumeId + ") success ------";
          logger.warn(title);
        }
        logPrintoutFile(title, null,
            "create only have one unit index :" + segmentIndex + "\ndata node :"
                + dataNodeEndPoint);
        logger.warn(
            "create only have one unit: \nindex :{}\ndata node :{}", segmentIndex,
            dataNodeEndPoint);
        Thread.sleep(3000);
      } catch (Exception e) {
        if (isOnlyOneCreateFirst) {
          title =
              "------ create only have one unit for volume:" + volume.getName() + "("
                  + volumeId + ") error fail ------";
          logger.error(title);
        }
        logPrintoutFile(title, null,
            "create only have one unit index :" + segmentIndex + ",data node :"
                + dataNodeEndPoint + ",find some error:" + e);
        logger.error(
            "create only have one unit index :{} ,data node :{},find some error:",
            segmentIndex, dataNodeEndPoint, e);
      }
      isOnlyOneCreateFirst = false;

    } //create only one
    //fix missing segment
    if (missIndexSet.size() > 0) {
      logger.warn(
          " *************** process missing segment **************");
      logPrintoutFile(
          "*************** process missing segment ******************", null, null);
    }
    boolean isFixMissCreateFirst = true;
    for (int segIndex : missIndexSet) {
      SegmentMetadata metadata = null;
      SegmentMembership exampleMembership = null;
      Set<InstanceId> members = null;
      List<SegmentMetadata> segmentMetadataList = volume.getSegments();
      for (SegmentMetadata segmentMetadata : segmentMetadataList) {
        metadata = segmentMetadata;
        SegmentStatus segmentStatus = segmentMetadata.getSegmentStatus();
        //ergodic the SegmentMetadata whether available, get members info
        if (segmentStatus != null && segmentStatus.available()) {
          exampleMembership = segmentMetadata.getLatestMembership();
          members = exampleMembership.getMembers();
          String valueAsString = new ObjectMapper().writeValueAsString(exampleMembership);
          logPrintoutFile(null,
              "find have segment available ", "++++++exampleMembership ："
                  + valueAsString);
          logger.warn("find have segment available, ++++++exampleMembership ：{}", valueAsString);
          break;
        }
      }
      logger.warn("try to create unit on missing seg index:{}. membership:{}", segIndex,
          exampleMembership);
      int createCount = 0;
      if (members != null) {
        logger.warn(
            "-index :{}, {} begin to create unit", segIndex, members.size());
        logPrintoutFile("-index :" + segIndex + "," + members.size()
            + " begin to create unit", null, null);
        for (InstanceId instanceId : members) {
          EndPoint dataNodeEndPoint = instanceStore.get(instanceId).getEndPoint();
          String title = null;
          try {
            createUnit.createUnit(volume, segIndex, 1, dataNodeEndPoint,
                exampleMembership, 0);
            if (isFixMissCreateFirst) {
              title = "------ create fix missing unit for volume:" + metadata.getVolume() + "("
                  + volumeId + ") success ------";
              logger.warn(title);
            }
            logPrintoutFile(title, null,
                "create fix missing unit index :" + segIndex + " \ndata node :"
                    + dataNodeEndPoint);
            logger.warn("create fix missing unit: \nindex :{}\ndata node :{}", segIndex, 
                dataNodeEndPoint);
            Thread.sleep(3000);
          } catch (Exception e) {

            if (isFixMissCreateFirst) {
              title = "------ create fix missing unit for volume:" + metadata.getVolume() + "("
                  + volumeId + ") error fail ------";
              logger.error(title);
            }
            logPrintoutFile(title, null,
                "create fix missing unit index :" + segIndex + " ,data node :"
                    + dataNodeEndPoint + ",find some error:" + e);
            logger.error(
                "create fix missing unit in index :{} ,data node :{}, find some error:",
                segIndex, dataNodeEndPoint, e);

          }
          isFixMissCreateFirst = false;
          createCount++;
          if (createCount >= volume.getVolumeType().getVotingQuorumSize()) {
            break;
          }
        }
        logPrintoutFile(null, null,
            "create " + createCount + " unit on seg index:" + segIndex + " over");
        logger.warn("create {} unit on seg index:{} over", createCount, segIndex);
      } else {
        logger.error(
            "the volume:{} SegmentMetadata is null , or the segmentStatus not available", volumeId);
        logPrintoutFile(null, null, "the volume:" + volumeId
            + " SegmentMetadata is null , or the segmentStatus not available");
      }
    }

  }

  /**
   * get InfoCenter Endpoint.
   *
   */
  private Instance getInfoCenterEndpoint(InstanceStore instanceStore) {
    Set<Instance> instanceMaster = instanceStore
        .getAll(PyService.INFOCENTER.getServiceName(), InstanceStatus.HEALTHY);
    Validate.isTrue(instanceMaster.isEmpty() || instanceMaster.size() == 1,
        "the master only one");
    for (Instance instance : instanceMaster) {
      return instance;
    }
    return null;
  }

  /**
   * Judgment acquisition and input ,If the acquired Endpoint is empty, the input Endpoint is used;
   * otherwise, the acquired Endpoint is used.
   *
   */
  private String getAcquisitionAndInputEndpoint(InstanceStore instanceStore,
      String inputEndpoint) {
    Instance instance = getInfoCenterEndpoint(instanceStore);
    if (null != instance && null != instance.getEndPoint()) {
      return instance.getEndPoint().getHostName();
    } else {
      return inputEndpoint;
    }
  }

  // The command line parser
  private static class CommandLineArgs {

    public static final String INFO_CENTER_ADDRESS = "--infoCenterAddress";
    public static final String INFO_CENTER_PORT = "--infoCenterPort";
    public static final String VOLUME_ID = "--volumeId";
    @Parameter(names = INFO_CENTER_ADDRESS,
        description = "the address of info center. like '10.0.2.101'", required = true)
    public String infoCenterAddress;
    @Parameter(names = INFO_CENTER_PORT,
        description = "the port of info center. default is 8020.", required = false)
    public int infoCenterPort;
    @Parameter(names = VOLUME_ID, description = "volume id , " 
        + "default is repair list volume if volume id is null", required = false)
    public long volumeId;

    /**
     * Boolean type is arity = 0 by default. In this case, the user should not set value for the
     * parameter. As long as there is -- autoCreate in the parameter, it will be parsed to true, and
     * if there is no, it will be parsed to false. But if you add Arity = 1, you need to specify
     * value explicitly
     */
    /*public static final String AUTO_CREATE = "--autoCreate";
    @Parameter(names = AUTO_CREATE,
        description = "whether auto create segment unit if 
        found segment only have one unit or segment missing. default is true",
        arity = 0, required = false)
    public boolean autoCreate = false;*/

    @Parameter(names = "--help", help = true)
    private boolean help;
  }

}
