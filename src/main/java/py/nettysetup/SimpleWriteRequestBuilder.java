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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang3.Validate;
import py.PbRequestResponseHelper;
import py.archive.segment.SegmentVersion;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.datanode.PyWriteRequest;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbWriteRequest.Builder;

public class SimpleWriteRequestBuilder {

  private static final Random randomizeData = new Random();

  private static Broadcastlog.PbMembership pbMembership;
  private static long requestId = RequestIdBuilder.get();
  private static RandomDataHolder dataHolder;

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
  public static PyWriteRequest generateSimpleWriteRequest(int eachRequestLength,
      int numberOfRequests) {
    Validate.isTrue(numberOfRequests > 0);

    Broadcastlog.PbWriteRequest.Builder writeRequestBuilder = Broadcastlog.PbWriteRequest
        .newBuilder();
    writeRequestBuilder.setRequestId(RequestIdBuilder.get());
    writeRequestBuilder.setVolumeId(RequestIdBuilder.get());
    writeRequestBuilder.setSegIndex(RandomUtils.nextInt(512));
    writeRequestBuilder.setFailTimes(0);
    writeRequestBuilder.setZombieWrite(false);
    writeRequestBuilder.setRequestTime(System.currentTimeMillis());

    writeRequestBuilder.setMembership(pbMembership);
    List<Broadcastlog.PbBroadcastLogManager> logManagerList = new ArrayList<>();
    writeRequestBuilder.addAllBroadcastManagers(logManagerList);

    ByteBuf data = null;
    for (int i = 0; i < numberOfRequests; i++) {
      Pair<Broadcastlog.PbWriteRequestUnit, ByteBuf> requestUnit = generatePbWriteRequestUnit(
          randomizeData,
          eachRequestLength, RandomUtils.nextInt(1024 * 1024 * 1024), 0);
      writeRequestBuilder.addRequestUnits(requestUnit.getFirst());
      ByteBuf bufferToSend = requestUnit.getSecond();
      data = data == null ? bufferToSend : Unpooled.wrappedBuffer(data, bufferToSend);
    }

    return new PyWriteRequest(writeRequestBuilder.build(), data);
  }

  /**
   * xx.
   */
  public static Pair<Broadcastlog.PbWriteRequestUnit, ByteBuf> generatePbWriteRequestUnit(
      Random randomForData,
      int length, long offset, int snapshotVersion) {
    Broadcastlog.PbWriteRequestUnit.Builder builder = Broadcastlog.PbWriteRequestUnit.newBuilder();
    builder.setLogUuid(RequestIdBuilder.get());
    builder.setLogId(0);
    builder.setOffset(offset);
    builder.setLength(length);
    // byte[] data = generateRandomData(randomForData, length);
    byte[] data = new byte[length];

    ByteBuf buffer = null;
    if (data != null) {
      builder.setChecksum(0L);
      buffer = Unpooled.wrappedBuffer(data);
    }

    return new Pair<>(builder.build(), buffer);
  }

  /**
   * xx.
   */
  public static PyWriteRequest generateDiscardSimpleWriteRequest(int eachRequestLength,
      int numberOfRequests) {
    Validate.isTrue(numberOfRequests > 0);

    Broadcastlog.PbWriteRequest.Builder writeRequestBuilder = Broadcastlog.PbWriteRequest
        .newBuilder();
    writeRequestBuilder.setRequestId(requestId);
    // underlying comment code just for debug
    // writeRequestBuilder.setRequestId(dynamicRequestId.incrementAndGet());
    writeRequestBuilder.setVolumeId(requestId);
    writeRequestBuilder.setSegIndex(RandomUtils.nextInt(512));
    writeRequestBuilder.setFailTimes(0);
    writeRequestBuilder.setZombieWrite(false);
    writeRequestBuilder.setRequestTime(System.currentTimeMillis());

    writeRequestBuilder.setMembership(pbMembership);
    generatePbWriteManagers(writeRequestBuilder, numberOfRequests);
    // List<Broadcastlog.PBBroadcastLogManager> logManagerList = new ArrayList<>();

    ByteBuf[] datas = new ByteBuf[numberOfRequests];
    final int snapshotVersion = 0;
    for (int i = 0; i < numberOfRequests; i++) {
      int offset = RandomUtils.nextInt(1024 * 1024 * 1024);
      Broadcastlog.PbWriteRequestUnit.Builder requestUnitBuilder = Broadcastlog.PbWriteRequestUnit
          .newBuilder();
      requestUnitBuilder.setLogUuid(requestId).setLogId(0).setOffset((long) offset)
          .setLength(eachRequestLength).setChecksum(0);

      // ByteBuf buffer = Unpooled.wrappedBuffer(new byte[requestLength]);
      ByteBuf buffer = getDataHolder(eachRequestLength).randomByteBufData();

      datas[i] = buffer;
      writeRequestBuilder.addRequestUnits(requestUnitBuilder.build());
    }

    CompositeByteBuf data = PooledByteBufAllocatorWrapper.INSTANCE.compositeBuffer(datas.length);
    data.addComponents(true, datas);

    return new PyWriteRequest(writeRequestBuilder.build(), data, false);
  }

  /**
   * xx.
   */
  public static Pair<Broadcastlog.PbWriteRequestUnit, ByteBuf> generateDiscardPbWriteRequestUnit(
      Random randomForData,
      int length, long offset, int snapshotVersion) {
    Broadcastlog.PbWriteRequestUnit.Builder builder = Broadcastlog.PbWriteRequestUnit.newBuilder();
    builder.setLogUuid(requestId);
    builder.setLogId(0);
    builder.setOffset(offset);
    builder.setLength(length);
    // byte[] data = generateRandomData(randomForData, length);
    byte[] data = new byte[length];

    ByteBuf buffer = null;
    if (data != null) {
      // builder.setChecksum(NetworkChecksumHelper.computeChecksum(data, 0, data.length));
      builder.setChecksum(0);
      buffer = Unpooled.wrappedBuffer(data);
    }

    return new Pair<>(builder.build(), buffer);
  }

  /**
   * xx.
   */
  public static void generatePbWriteManagers(Builder writeRequestBuilder, int generateCount) {
    Broadcastlog.PbBroadcastLogManager.Builder managerBuilder = Broadcastlog.PbBroadcastLogManager
        .newBuilder();
    managerBuilder.setRequestId(requestId);
    for (int i = 0; i < generateCount; i++) {
      Broadcastlog.PbBroadcastLog.Builder logBuilder = Broadcastlog.PbBroadcastLog.newBuilder();
      logBuilder.setLogUuid(requestId);
      logBuilder.setLogId(requestId);
      logBuilder.setOffset(requestId);
      logBuilder.setChecksum(0);
      logBuilder.setLength(1024);
      logBuilder.setLogStatus(Broadcastlog.PbBroadcastLogStatus.CREATED);
      managerBuilder.addBroadcastLogs(logBuilder.build());
    }
    writeRequestBuilder.addBroadcastManagers(managerBuilder.build());
  }

  /**
   * xx.
   */
  public static byte[] generateRandomData(Random randomForData, int length) {
    if (length == 0) {
      return null;
    }
    byte[] data = new byte[length];

    randomForData.nextBytes(data);
    for (int i = 0; i < data.length; i++) {
      if (data[i] == (byte) 0) {
        // I don't like 0. We like to use 0 to indicate this byte has not been used
        data[i] = (byte) 1;
      }
    }

    return data;
  }

  /**
   * xx.
   */
  public static RandomDataHolder getDataHolder(int size) {
    if (dataHolder == null) {
      dataHolder = new RandomDataHolder(128, size);
    }

    return dataHolder;
  }
}
