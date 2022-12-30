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

package py.nettysetup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.Validate;
import py.PbRequestResponseHelper;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog;

public class SimpleReadRequestBuilder {

  private static Broadcastlog.PbMembership pbMembership;
  private static long requestId = RequestIdBuilder.get();

  static {
    SegmentVersion segmentVersion = new SegmentVersion(1, 0);
    Collection<InstanceId> secondaries = new ArrayList<>();
    InstanceId primary = new InstanceId(RequestIdBuilder.get());
    InstanceId secondary1 = new InstanceId(RequestIdBuilder.get());
    InstanceId secondary2 = new InstanceId(RequestIdBuilder.get());
    secondaries.add(secondary1);
    secondaries.add(secondary2);

    SegmentMembership membership = new SegmentMembership(segmentVersion, primary, secondaries);
    pbMembership = PbRequestResponseHelper.buildPbMembershipFrom(membership);
  }

  /**
   * xx.
   */
  public static Broadcastlog.PbReadRequest generateSimpleReadRequest(int eachRequestLength,
      int numberOfRequests) {
    Validate.isTrue(numberOfRequests > 0);
    Broadcastlog.PbReadRequest.Builder readRequestBuilder = Broadcastlog.PbReadRequest.newBuilder();
    readRequestBuilder.setRequestId(requestId);
    readRequestBuilder.setVolumeId(requestId);
    readRequestBuilder.setSegIndex(RandomUtils.nextInt(512));
    readRequestBuilder.setFailTimes(0);
    readRequestBuilder.setReadCause(Broadcastlog.ReadCause.FETCH);
    readRequestBuilder.setMembership(pbMembership);
    List<Broadcastlog.PbReadRequestUnit> requestUnitList = new ArrayList<>();
    List<Long> logsToCommit = new ArrayList<>();
    for (int i = 0; i < numberOfRequests; i++) {
      Broadcastlog.PbReadRequestUnit.Builder unitBuilder = Broadcastlog.PbReadRequestUnit
          .newBuilder();
      int offset = RandomUtils.nextInt(1024 * 1024 * 1024);
      unitBuilder.setOffset(offset);
      unitBuilder.setLength(eachRequestLength);
      requestUnitList.add(unitBuilder.build());
      logsToCommit.add(requestId);
    }
    readRequestBuilder.addAllRequestUnits(requestUnitList);
    readRequestBuilder.addAllLogsToCommit(logsToCommit);

    return readRequestBuilder.build();
  }

}
