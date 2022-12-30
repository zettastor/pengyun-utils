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

package py.memorynbdserver.mocktools;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
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

public class MockDatanodeNettyIface implements AsyncDataNode.AsyncIface {

  public static final Logger logger = LoggerFactory.getLogger(MockDatanodeNettyIface.class);
  private ByteBuffer byteBuffer;
  private boolean isPrimary;

  public MockDatanodeNettyIface(ByteBuffer byteBuffer, boolean isPrimary) {
    this.byteBuffer = byteBuffer;
    this.isPrimary = isPrimary;
  }

  @Override
  public void ping(MethodCallback<Object> callback) {

  }

  @Override
  public void write(PyWriteRequest request, MethodCallback<Broadcastlog.PbWriteResponse> callback) {
    try {
      //request.initDataOffsets();
      Broadcastlog.PbWriteRequest pbWriteRequest = request.getMetadata();
      // commit logs
      List<Broadcastlog.PbBroadcastLogManager> managersToBeCommitted = new ArrayList<>();
      for (Broadcastlog.PbBroadcastLogManager manager : pbWriteRequest.getBroadcastManagersList()) {
        Broadcastlog.PbBroadcastLogManager.Builder builder = Broadcastlog.PbBroadcastLogManager
            .newBuilder();
        builder.setRequestId(manager.getRequestId());
        // add these committed logs to plal engine so that it can apply them
        for (Broadcastlog.PbBroadcastLog commitLog : manager.getBroadcastLogsList()) {
          Broadcastlog.PbBroadcastLog.Builder newBuilder = Broadcastlog.PbBroadcastLog.newBuilder();

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

      int segmentIndex = pbWriteRequest.getSegIndex();
      long baseOffset = segmentIndex * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE;
      int index = 0;
      for (Broadcastlog.PbWriteRequestUnit writeUnit : pbWriteRequest.getRequestUnitsList()) {
        index++;
        Broadcastlog.PbIoUnitResult pbioUnitResult;
        Broadcastlog.PbWriteResponseUnit.Builder builder = Broadcastlog.PbWriteResponseUnit
            .newBuilder();
        if (isPrimary) {
          pbioUnitResult = Broadcastlog.PbIoUnitResult.PRIMARY_COMMITTED;
          builder.setLogId(MockCoordinatorBuilder.logIdGenerator.incrementAndGet());
        } else {
          pbioUnitResult = Broadcastlog.PbIoUnitResult.OK;
          builder.setLogId(0);
        }
        builder.setLogUuid(writeUnit.getLogUuid());
        builder.setLogResult(pbioUnitResult);
        responseBuilder.addResponseUnits(builder.build());
      }
      request.getData().release();

      callback.complete(responseBuilder.build());
    } catch (Throwable t) {
      logger.error("####caught an exception", t);
    }

  }

  @Override
  public void read(Broadcastlog.PbReadRequest request, MethodCallback<PyReadResponse> callback) {
    Broadcastlog.PbReadResponse.Builder responseBuilder = Broadcastlog.PbReadResponse.newBuilder();
    responseBuilder.setRequestId(request.getRequestId());

    long baseOffset = request.getSegIndex() * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE;
    ByteBuf data = null;
    for (Broadcastlog.PbReadRequestUnit pbReadRequestUnit : request.getRequestUnitsList()) {

      if (isPrimary) {
        long offset = baseOffset + pbReadRequestUnit.getOffset();
        byte[] bytes = new byte[pbReadRequestUnit.getLength()];
        // get data
        synchronized (byteBuffer) {
          Validate.isTrue(offset >= 0);
          try {
            byteBuffer.position((int) offset);
            byteBuffer.get(bytes);
          } catch (Exception e) {
            throw e;
          }
        }

        ByteBuf tmpBuf = Unpooled.wrappedBuffer(bytes);
        data = (data == null ? tmpBuf : Unpooled.wrappedBuffer(data, tmpBuf));

        responseBuilder.addResponseUnits(PbRequestResponseHelper
            .buildPbReadResponseUnitFrom(pbReadRequestUnit, Broadcastlog.PbIoUnitResult.OK));
      }
    }
    PyReadResponse pyReadResponse = new PyReadResponse(responseBuilder.build(), data);
    callback.complete(pyReadResponse);
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
  public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
      MethodCallback<Commitlog.PbCommitlogResponse> callback) {
  }

  @Override
  public void discard(Broadcastlog.PbWriteRequest request,
      MethodCallback<Broadcastlog.PbWriteResponse> callback) {

    try {
      //request.initDataOffsets();
      Broadcastlog.PbWriteRequest pbWriteRequest = request;
      // commit logs
      List<Broadcastlog.PbBroadcastLogManager> managersToBeCommitted = new ArrayList<>();
      for (Broadcastlog.PbBroadcastLogManager manager : pbWriteRequest.getBroadcastManagersList()) {
        Broadcastlog.PbBroadcastLogManager.Builder builder = Broadcastlog.PbBroadcastLogManager
            .newBuilder();
        builder.setRequestId(manager.getRequestId());
        // add these committed logs to plal engine so that it can apply them
        for (Broadcastlog.PbBroadcastLog commitLog : manager.getBroadcastLogsList()) {
          Broadcastlog.PbBroadcastLog.Builder newBuilder = Broadcastlog.PbBroadcastLog.newBuilder();

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

      int segmentIndex = pbWriteRequest.getSegIndex();
      long baseOffset = segmentIndex * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE;
      int index = 0;
      for (Broadcastlog.PbWriteRequestUnit writeUnit : pbWriteRequest.getRequestUnitsList()) {

        if (isPrimary) {
          long offset = baseOffset + writeUnit.getOffset();
        }

        index++;
        Broadcastlog.PbIoUnitResult pbioUnitResult;
        Broadcastlog.PbWriteResponseUnit.Builder builder = Broadcastlog.PbWriteResponseUnit
            .newBuilder();
        if (isPrimary) {
          pbioUnitResult = Broadcastlog.PbIoUnitResult.PRIMARY_COMMITTED;
          builder.setLogId(MockCoordinatorBuilder.logIdGenerator.incrementAndGet());
        } else {
          pbioUnitResult = Broadcastlog.PbIoUnitResult.OK;
          builder.setLogId(0);
        }
        builder.setLogUuid(writeUnit.getLogUuid());
        builder.setLogResult(pbioUnitResult);
        responseBuilder.addResponseUnits(builder.build());
      }
      // request.getData().release();

      callback.complete(responseBuilder.build());
    } catch (Throwable t) {
      logger.error("####caught an exception", t);
    }
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
