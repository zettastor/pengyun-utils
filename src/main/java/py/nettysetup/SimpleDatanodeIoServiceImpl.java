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
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.archive.segment.SegId;
import py.membership.SegmentMembership;
import py.netty.core.MethodCallback;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.PyCopyPageRequest;
import py.netty.datanode.PyReadResponse;
import py.netty.datanode.PyWriteRequest;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Commitlog;
import py.proto.Commitlog.PbCommitlogRequest;
import py.proto.Commitlog.PbCommitlogResponse;

public class SimpleDatanodeIoServiceImpl implements AsyncDataNode.AsyncIface {

  private static final Logger logger = LoggerFactory.getLogger(SimpleDatanodeIoServiceImpl.class);
  private static final AtomicLong logIdGenerator = new AtomicLong(0);
  private static RandomDataHolder dataHolder;
  private final ByteBufAllocator allocator;

  /**
   * xx.
   */
  public SimpleDatanodeIoServiceImpl(ByteBufAllocator allocator) {
    this.allocator = allocator;
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

  @Override
  public void ping(MethodCallback<Object> callback) {

  }

  @Override
  public void write(PyWriteRequest writeRequest,
      MethodCallback<Broadcastlog.PbWriteResponse> callback) {
    try {
      Broadcastlog.PbWriteRequest request = writeRequest.getMetadata();

      SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
      logger.debug("@@@@ got one write request:<<{}>> at:{}", request.getRequestId(), segId);
      SegmentMembership requestMembership = PbRequestResponseHelper
          .buildMembershipFrom(request.getMembership());

      try {
        //request.initDataOffsets();
        Broadcastlog.PbWriteRequest pbWriteRequest = writeRequest.getMetadata();
        // commit logs
        List<Broadcastlog.PbBroadcastLogManager> managersToBeCommitted = new ArrayList<>();
        for (Broadcastlog.PbBroadcastLogManager manager : pbWriteRequest
            .getBroadcastManagersList()) {
          Broadcastlog.PbBroadcastLogManager.Builder builder = Broadcastlog.PbBroadcastLogManager
              .newBuilder();
          builder.setRequestId(manager.getRequestId());
          // add these committed logs to plal engine so that it can apply them
          for (Broadcastlog.PbBroadcastLog commitLog : manager.getBroadcastLogsList()) {
            Broadcastlog.PbBroadcastLog.Builder newBuilder = Broadcastlog.PbBroadcastLog
                .newBuilder();

            newBuilder.setLogUuid(commitLog.getLogUuid());
            newBuilder.setLogId(commitLog.getLogId());
            newBuilder.setOffset(commitLog.getOffset());
            newBuilder.setChecksum(commitLog.getChecksum());
            newBuilder.setLength(commitLog.getLength());
            newBuilder.setLogStatus(Broadcastlog.PbBroadcastLogStatus.COMMITTED);

            builder.addBroadcastLogs(newBuilder.build());
          }
          managersToBeCommitted.add(builder.build());
        }

        Broadcastlog.PbWriteResponse.Builder responseBuilder = Broadcastlog.PbWriteResponse
            .newBuilder();
        responseBuilder.setRequestId(pbWriteRequest.getRequestId());
        responseBuilder.addAllLogManagersToCommit(managersToBeCommitted);

        for (Broadcastlog.PbWriteRequestUnit writeUnit : pbWriteRequest.getRequestUnitsList()) {
          Broadcastlog.PbIoUnitResult pbioUnitResult;
          Broadcastlog.PbWriteResponseUnit.Builder builder = Broadcastlog.PbWriteResponseUnit
              .newBuilder();
          pbioUnitResult = Broadcastlog.PbIoUnitResult.PRIMARY_COMMITTED;
          builder.setLogId(logIdGenerator.incrementAndGet());
          builder.setLogUuid(writeUnit.getLogUuid());
          builder.setLogResult(pbioUnitResult);
          responseBuilder.addResponseUnits(builder.build());
        }

        callback.complete(responseBuilder.build());
      } catch (Throwable t) {
        logger.error("caught an exception", t);
      }
    } finally {
      logger.info("nothing need to do here");
    }
  }

  @Override
  public void read(Broadcastlog.PbReadRequest request, MethodCallback<PyReadResponse> callback) {
    try {
      Broadcastlog.PbReadResponse.Builder responseBuilder = Broadcastlog.PbReadResponse
          .newBuilder();
      responseBuilder.setRequestId(request.getRequestId());

      int totalLen = 0;
      for (Broadcastlog.PbReadRequestUnit pbReadRequestUnit : request.getRequestUnitsList()) {
        totalLen += pbReadRequestUnit.getLength();

        responseBuilder.addResponseUnits(PbRequestResponseHelper
            .buildPbReadResponseUnitFrom(pbReadRequestUnit, Broadcastlog.PbIoUnitResult.OK));
      }

      ByteBuf data = allocator.directBuffer(totalLen);
      data.writerIndex(totalLen);

      for (Broadcastlog.PbReadRequestUnit pbReadRequestUnit : request.getRequestUnitsList()) {
        // slice out from the original buf
        ByteBuf dstTempBuf = data.readSlice(pbReadRequestUnit.getLength());
        dstTempBuf.writerIndex(0);
        ByteBuf tmpBuf = getDataHolder(pbReadRequestUnit.getLength()).randomByteBufData();
        tmpBuf.readBytes(dstTempBuf);
        /* Do we need to release dstTempBuf??? */
      }

      data.readerIndex(0);

      PyReadResponse pyReadResponse = new PyReadResponse(responseBuilder.build(), data);
      callback.complete(pyReadResponse);
    } finally {
      logger.info("nothing need to do here");
    }
  }

  @Override
  public void copy(PyCopyPageRequest request, MethodCallback<PbCopyPageResponse> callback) {

  }

  @Override
  public void check(Broadcastlog.PbCheckRequest request,
      MethodCallback<Broadcastlog.PbCheckResponse> callback) {

  }

  @Override
  public void giveYouLogId(Broadcastlog.GiveYouLogIdRequest request,
      MethodCallback<Broadcastlog.GiveYouLogIdResponse> callback) {

  }

  @Override
  public void getMembership(Broadcastlog.PbGetMembershipRequest request,
      MethodCallback<Broadcastlog.PbGetMembershipResponse> callback) {

  }

  @Override
  public void addOrCommitLogs(PbCommitlogRequest request,
      MethodCallback<PbCommitlogResponse> callback) {

  }

  @Override
  public void discard(Broadcastlog.PbWriteRequest request,
      MethodCallback<Broadcastlog.PbWriteResponse> callback) {

  }

  @Override
  public void startOnlineMigration(Commitlog.PbStartOnlineMigrationRequest request,
      MethodCallback<Commitlog.PbStartOnlineMigrationResponse> callback) {

  }

  @Override
  public void syncLog(PbAsyncSyncLogsBatchRequest request,
      MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {

  }

  @Override
  public void backwardSyncLog(PbBackwardSyncLogsRequest request,
      MethodCallback<PbBackwardSyncLogsResponse> callback) {

  }
}
