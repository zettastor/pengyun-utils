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

package py.utils.console;

import java.util.HashSet;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.infocenter.client.InformationCenterClientFactory;
import py.thrift.icshare.CreateVolumeRequest;
import py.thrift.icshare.CreateVolumeResponse;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.AccountTypeThrift;
import py.thrift.share.CacheTypeThrift;
import py.thrift.share.CreateAccountRequest;
import py.thrift.share.CreateAccountResponse;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.LaunchDriverRequestThrift;
import py.thrift.share.LaunchDriverResponseThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.thrift.share.VolumeStatusThrift;
import py.thrift.share.VolumeTypeThrift;

/**
 * this class used to contract with controlcenter to create a volume as the system does not create
 * the volume right now, so need to ask infocenter the volume whether create ok or not.
 */
public class ConsoleCenter {

  private static final Log logger = LogFactory.getLog(ConsoleCenter.class);
  private InformationCenterClientFactory informationCenterClientFactory = 
      new InformationCenterClientFactory(
      1);

  private int timeout = 60000;

  private EndPoint controlEps;

  private EndPoint infoEps;

  public EndPoint getControlEps() {
    return controlEps;
  }

  public void setControlEps(EndPoint controlEps) {
    this.controlEps = controlEps;
  }

  public EndPoint getInfoEps() {
    return infoEps;
  }

  public void setInfoEps(EndPoint infoEps) {
    this.infoEps = infoEps;
  }

  /**
   * xx.
   */
  public long generateId() {
    long id = UUID.randomUUID().getLeastSignificantBits();
    if (id < 0) {
      id = id + Long.MAX_VALUE;
    }
    return id;
  }

  /**
   * xx.
   */
  public CreateVolumeResponse createVolume(String volumeName, long volumeSize, long accountId,
      long volumeId) throws Exception {
    InformationCenter.Iface client = null;
    CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest();
    if (volumeId != 0) {
      createVolumeRequest.setVolumeId(volumeId);
    }
    createVolumeRequest.setName(volumeName);
    createVolumeRequest.setRequestId(generateId());
    createVolumeRequest.setVolumeSize(volumeSize);
    createVolumeRequest.setVolumeType(VolumeTypeThrift.REGULAR);
    createVolumeRequest.setAccountId(accountId);
    createVolumeRequest.setEnableLaunchMultiDrivers(true);
    CreateVolumeResponse createVolumeResponse;
    try {
      client = informationCenterClientFactory.build(controlEps, timeout).getClient();
      logger.info("start to create the volume");
      createVolumeResponse = client.createVolume(createVolumeRequest);
      while (!isVolumeCreated(createVolumeResponse.getVolumeId(), accountId)) {
        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          logger.error("caught an exception while waiting for volume being created", e);
        }
      }
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
    return createVolumeResponse;
  }

  /**
   * xx.
   */
  public CreateAccountResponse createAccount(String accountName, String password, long accountId)
      throws TException {
    CreateAccountRequest createAccountRequest = new CreateAccountRequest(accountName, password,
        AccountTypeThrift.Regular, accountId, new HashSet<>());
    if (accountId != 0) {
      createAccountRequest.setAccountId(accountId);
    }
    CreateAccountResponse response = null;
    InformationCenter.Iface client;
    try {
      client = informationCenterClientFactory.build(infoEps, timeout).getClient();
      response = client.createAccount(createAccountRequest);
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }
    return response;
  }

  /**
   * xx.
   */
  public boolean isVolumeCreated(long volumeId, long accountId) {
    boolean isVolumeCreated = false;
    InformationCenter.Iface client = null;
    try {
      client = informationCenterClientFactory.build(infoEps, timeout).getClient();
      GetVolumeRequest getVolumeFromInfoCenterRequest = new GetVolumeRequest();
      getVolumeFromInfoCenterRequest.setRequestId(generateId());
      getVolumeFromInfoCenterRequest.setVolumeId(volumeId);
      getVolumeFromInfoCenterRequest.setAccountId(accountId);
      GetVolumeResponse getVolumeFromInfoCenterResponse = client
          .getVolume(getVolumeFromInfoCenterRequest);
      VolumeStatusThrift volumeStatus = getVolumeFromInfoCenterResponse.getVolumeMetadata()
          .getVolumeStatus();
      logger.debug("volume status is " + volumeStatus);
      if (VolumeStatusThrift.Available.equals(getVolumeFromInfoCenterResponse.getVolumeMetadata()
          .getVolumeStatus()) 
          || VolumeStatusThrift.Stable.equals(getVolumeFromInfoCenterResponse.getVolumeMetadata()
              .getVolumeStatus())) {
        logger.info("volume status is available now");
        isVolumeCreated = true;
      }
    } catch (VolumeNotFoundExceptionThrift e) {
      logger.error("caught an exception", e);
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }
    return isVolumeCreated;
  }

  /**
   * xx.
   */
  public LaunchDriverResponseThrift launchDriver(long volumeId, String volumeName, long accountId,
      DriverTypeThrift driverTypeThrift) {
    InformationCenter.Iface client = null;
    try {
      client = informationCenterClientFactory.build(controlEps, timeout).getClient();
      LaunchDriverRequestThrift request = new LaunchDriverRequestThrift();
      request.setRequestId(RequestIdBuilder.get());
      request.setVolumeId(volumeId);
      request.setAccountId(accountId);
      request.setDriverType(driverTypeThrift);
      logger.debug("the launch console request is " + request);
      LaunchDriverResponseThrift response = client.launchDriver(request);
      return response;
    } catch (Exception e) {
      logger.error("Exception catch ", e);
    }
    return null;
  }

  /**
   * xx.
   */
  public void close() {
    if (informationCenterClientFactory != null) {
      informationCenterClientFactory.close();
      informationCenterClientFactory = null;
    }
  }
}
