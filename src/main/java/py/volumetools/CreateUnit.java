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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.client.thrift.GenericThriftClientFactory;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.exception.GenericThriftClientFactoryException;
import py.exception.SegmentUnitCreateException;
import py.infocenter.client.InformationCenterClientFactory;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.AsyncIface;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class CreateUnit {

  private static final Logger logger = LoggerFactory.getLogger(CreateUnit.class);
  private final int timeout = 20000;
  private final int maxWaitTimeMs = 90000;
  private long accountId = SUPERADMIN_ACCOUNT_ID;
  private InformationCenterClientFactory infoCenterClientFactory;
  private InformationCenter.Iface informationCenter;
  private GenericThriftClientFactory<AsyncIface> dataNodeAsyncClientFactory;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {

    // initialize the parameter passed in.
    CommandLineArgs cliArgs = new CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);
    if (cliArgs.help) {
      commander.setProgramName("java -jar CreateUnit.jar");
      commander.usage();
      return;
    }

    int infoCenterPort = cliArgs.infoCenterPort;
    if (infoCenterPort == 0) {
      infoCenterPort = 8020;
    }

    String dataNodeAddress = cliArgs.dataNodeAddress;
    int dataNodePort = cliArgs.dataNodePort;
    if (dataNodePort == 0) {
      dataNodePort = 10011;
    }

    long volumeId = cliArgs.volumeId;
    int segmentIndex = cliArgs.segmentIndex;

    int needCreateMembership = cliArgs.needCreateMembership;

    int unitType = cliArgs.unitType;
    if (unitType == 0) {
      unitType = 1;
    }
    String infoCenterAddress = cliArgs.infoCenterAddress;

    logger.warn("the :{} {} {} {} {} {}", infoCenterAddress, infoCenterPort, dataNodeAddress,
        dataNodePort, volumeId, segmentIndex);
    CreateUnit createUnit = new CreateUnit();
    try {
      EndPoint infoCenterEndPoint = new EndPoint(infoCenterAddress, infoCenterPort);
      EndPoint dataNodeEndPoint = new EndPoint(dataNodeAddress, dataNodePort);
      VolumeMetadata volumeMetadata = createUnit
          .getVolumeInfo(volumeId, segmentIndex, infoCenterEndPoint);
      if (volumeMetadata == null) {
        logger.warn("can not get the volume :{} info", volumeId);
      } else {
        logger.warn("the Membership is :{}",
            new ObjectMapper().writeValueAsString(volumeMetadata.getMembership(0).getMembers()));

        createUnit.createUnit(volumeMetadata, segmentIndex, unitType, dataNodeEndPoint, null,
            needCreateMembership);
      }
    } catch (Exception e) {
      logger.warn("create unit for volume:{}, find some error:", volumeId, e);
    }

    System.exit(0);
  }

  /**
   * xx.
   */
  public void createUnit(VolumeMetadata volumeMetadata, int segmentIndex, int segmentUnitType,
      EndPoint dataNodeEndPoint, SegmentMembership membership, int needCreateMembership)
      throws GenericThriftClientFactoryException, SegmentUnitCreateException {
    SegmentUnitTypeThrift segmentUnitTypeThrift = null;
    if (segmentUnitType == 0) {
      segmentUnitTypeThrift = SegmentUnitTypeThrift.Normal;
    } else {
      segmentUnitTypeThrift = SegmentUnitTypeThrift.findByValue(segmentUnitType);
    }
    py.thrift.datanode.service.CreateSegmentUnitRequest createSegmentUnitRequest = 
        new py.thrift.datanode.service.CreateSegmentUnitRequest(
        RequestIdBuilder.get(), volumeMetadata.getVolumeId(), segmentIndex,
        volumeMetadata.getVolumeType().getVolumeTypeThrift(),
        volumeMetadata.getStoragePoolId(),
        segmentUnitTypeThrift, volumeMetadata.getSegmentWrappCount(),
        volumeMetadata.isEnableLaunchMultiDrivers(),
        RequestResponseHelper.buildThriftVolumeSource(volumeMetadata.getVolumeSource()));

    try {
      SegmentMembership membershipTemp = null;
      int segmentIndexTemp = segmentIndex;

      if (membership == null) {
        membershipTemp = volumeMetadata.getMembership(segmentIndex);

        while (membershipTemp == null && needCreateMembership == 1) {
          logger.warn("for volume :{}, in index :{}, can not get the membership",
              volumeMetadata.getVolumeId(), segmentIndexTemp);
          segmentIndexTemp++;
          membershipTemp = volumeMetadata.getMembership(segmentIndexTemp);
          if (membershipTemp != null) {
            logger
                .warn("for volume :{}, in index :{},get the membership for :{}, the membership {} ",
                    volumeMetadata.getVolumeId(), segmentIndex, segmentIndexTemp,
                    membershipTemp);
            break;
          }
        }

      } else {
        membershipTemp = membership;
      }

      SegId segId = new SegId(volumeMetadata.getVolumeId(), segmentIndexTemp);

      createSegmentUnitRequest.setInitMembership(RequestResponseHelper
          .buildThriftMembershipFrom(segId,
              membershipTemp));

      CountDownLatch latch = new CountDownLatch(1);
      AtomicInteger numGoodResponses = new AtomicInteger(0);
      dataNodeAsyncClientFactory = dataNodeAsyncClientFactory();
      AsyncIface dataNodeClient = dataNodeAsyncClientFactory
          .generateAsyncClient(dataNodeEndPoint, timeout);
      dataNodeClient.createSegmentUnit(createSegmentUnitRequest,
          new CreateSegmentUnitMethodCallback(latch, numGoodResponses));
      logger.warn("try to createSegmentUnit in endPoint: {}", dataNodeEndPoint);
      logger.warn("createSegmentUnit createSegmentUnitRequest: {}", createSegmentUnitRequest);

      if (!latch.await(maxWaitTimeMs, TimeUnit.MILLISECONDS)) {
        logger.error("createSegments request timeout!");
      }

      if (numGoodResponses.get() > 0) {
        logger.warn("dataNodeClient {} createSegmentUnit ok",
            dataNodeEndPoint);
      } else {
        logger.warn("dataNodeClient {} createSegmentUnit error",
            dataNodeEndPoint);
      }

    } catch (Exception e) {
      logger.warn("create unit, find some error:", e);
      throw new SegmentUnitCreateException("create unit, find some error:" + e);

    }
  }

  /**
   * xx.
   */
  public VolumeMetadata getVolumeInfo(long volumeId, int segmentIndex, EndPoint endPoint)
      throws Exception {
    try {
      infoCenterClientFactory = informationCenterClientFactory();
      informationCenter = infoCenterClientFactory.build(endPoint, 6000).getClient();
      logger.warn("try to get info center client in endPoint: {}", endPoint);

      /* getVolume **/
      GetVolumeRequest getVolumeRequest = new GetVolumeRequest();
      getVolumeRequest.setRequestId(RequestIdBuilder.get());
      getVolumeRequest.setAccountId(accountId);
      getVolumeRequest.setVolumeId(volumeId);
      getVolumeRequest.setStartSegmentIndex(segmentIndex);
      getVolumeRequest.setPaginationNumber(10);

      GetVolumeResponse response = informationCenter.getVolume(getVolumeRequest);
      VolumeMetadata volumeMetadata = RequestResponseHelper
          .buildVolumeFrom(response.getVolumeMetadata());
      return volumeMetadata;

    } catch (Exception e) {
      logger.error("catch an exception when get volume: {}, the error:", volumeId, e);
      throw e;
    }
  }

  /**
   * xx.
   */
  public InformationCenterClientFactory informationCenterClientFactory() throws Exception {
    InformationCenterClientFactory factory = new InformationCenterClientFactory(1);
    factory.setInstanceName(PyService.INFOCENTER.getServiceName());
    return factory;
  }

  public GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory() {
    return GenericThriftClientFactory.create(DataNodeService.AsyncIface.class, 1);
  }

  // The command line parser
  private static class CommandLineArgs {

    public static final String INFO_CENTER_ADDRESS = "--infoCenterAddress";
    public static final String INFO_CENTER_PORT = "--infoCenterPort";
    public static final String DATA_NODE_ADDRESS = "--dataNodeAddress";
    public static final String DATA_NODE_PORT = "--dataNodePort";
    public static final String VOLUME_ID = "--volumeId";
    public static final String SEGMENT_INDEX = "--segmentIndex";
    public static final String NEED_CREATE_MEMBERSHIP = "--needCreateMembership";
    public static final String UNIT_TYPE = "--unitType";
    @Parameter(names = INFO_CENTER_ADDRESS,
        description = "the address of info center. like '10.0.2.101'", required = true)
    public String infoCenterAddress;
    @Parameter(names = INFO_CENTER_PORT,
        description = "the port of info center. default is 8020.", required = false)
    public int infoCenterPort;
    @Parameter(names = DATA_NODE_ADDRESS,
        description = "the address of data node. like '10.0.2.101'", required = true)
    public String dataNodeAddress;
    @Parameter(names = DATA_NODE_PORT,
        description = "the port of data node. default is 8020.", required = false)
    public int dataNodePort;
    @Parameter(names = VOLUME_ID, description = "volume id", required = true)
    public long volumeId;
    @Parameter(names = SEGMENT_INDEX, description = "segment index", required = true)
    public int segmentIndex;
    @Parameter(names = NEED_CREATE_MEMBERSHIP, description = 
        "need Create Membership or not, eg:1", required = false)
    public int needCreateMembership;
    @Parameter(names = UNIT_TYPE, description = "unit type, eg:1", required = false)
    public int unitType;

    @Parameter(names = "--help", help = true)
    private boolean help;
  }
}
