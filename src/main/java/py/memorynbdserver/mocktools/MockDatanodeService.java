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
import java.util.concurrent.atomic.AtomicLong;
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
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Commitlog;

public class MockDatanodeService implements AsyncDataNode.AsyncIface {

  public static final Logger logger = LoggerFactory.getLogger(MockDatanodeService.class);

  private ByteBuffer byteBuffer;
  private AtomicLong fakeLogIdGenerator = new AtomicLong(0);

  public MockDatanodeService(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
  }

  @Override
  public void ping(MethodCallback<Object> callback) {
  }

  private void fakeWrite(PyWriteRequest request,
      MethodCallback<Broadcastlog.PbWriteResponse> callback) {
    try {
      Broadcastlog.PbWriteRequest pbWriteRequest = request.getMetadata();
      // commit logs
      List<PbBroadcastLogManager> broadcastManagersList = pbWriteRequest.getBroadcastManagersList();
      List<PbBroadcastLogManager> managersToBeCommitted = new ArrayList<>(
          broadcastManagersList.size());

      for (Broadcastlog.PbBroadcastLogManager manager : broadcastManagersList) {
        Broadcastlog.PbBroadcastLogManager.Builder builder = Broadcastlog.PbBroadcastLogManager
            .newBuilder();
        builder.setRequestId(manager.getRequestId());

        // add these committed logs to plal engine so that it can apply them
        for (Broadcastlog.PbBroadcastLog commitLog : manager.getBroadcastLogsList()) {
          Broadcastlog.PbBroadcastLog broadcastLog = Broadcastlog.PbBroadcastLog.newBuilder()
              .setLogUuid(commitLog.getLogUuid())
              .setLogId(commitLog.getLogId())
              .setOffset(commitLog.getOffset())
              .setChecksum(commitLog.getChecksum())
              .setLength(commitLog.getLength())
              .setLogStatus(Broadcastlog.PbBroadcastLogStatus.COMMITTED)
              .build();

          builder.addBroadcastLogs(broadcastLog);
        }

        managersToBeCommitted.add(builder.build());
      }

      Broadcastlog.PbWriteResponse.Builder responseBuilder = Broadcastlog.PbWriteResponse
          .newBuilder();
      responseBuilder.setRequestId(pbWriteRequest.getRequestId())
          .addAllLogManagersToCommit(managersToBeCommitted);

      for (Broadcastlog.PbWriteRequestUnit writeUnit : pbWriteRequest.getRequestUnitsList()) {
        Broadcastlog.PbWriteResponseUnit writeResponseUnit = Broadcastlog.PbWriteResponseUnit
            .newBuilder()
            .setLogId(fakeLogIdGenerator.incrementAndGet())
            .setLogUuid(writeUnit.getLogUuid())
            .setLogResult(Broadcastlog.PbIoUnitResult.PRIMARY_COMMITTED)
            .build();

        responseBuilder.addResponseUnits(writeResponseUnit);
      }

      callback.complete(responseBuilder.build());
    } catch (Throwable t) {
      logger.error("####caught an exception", t);
    }
  }

  @Override
  public void write(PyWriteRequest request, MethodCallback<Broadcastlog.PbWriteResponse> callback) {
    fakeWrite(request, callback);
  }

  @Override
  public void read(Broadcastlog.PbReadRequest request, MethodCallback<PyReadResponse> callback) {
    Broadcastlog.PbReadResponse.Builder responseBuilder = Broadcastlog.PbReadResponse.newBuilder();
    responseBuilder.setRequestId(request.getRequestId());

    long baseOffset = request.getSegIndex() * MockCoordinatorBuilder.DEFAULT_SEGMENT_SIZE;
    ByteBuf data = null;
    for (Broadcastlog.PbReadRequestUnit pbReadRequestUnit : request.getRequestUnitsList()) {

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
  public void syncLog(PbAsyncSyncLogsBatchRequest request,
      MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {

  }

  @Override
  public void backwardSyncLog(PbBackwardSyncLogsRequest request,
      MethodCallback<PbBackwardSyncLogsResponse> callback) {

  }

  @Override
  public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
      MethodCallback<Commitlog.PbCommitlogResponse> callback) {

  }

  @Override
  public void discard(Broadcastlog.PbWriteRequest request,
      MethodCallback<Broadcastlog.PbWriteResponse> callback) {

  }

  @Override
  public void startOnlineMigration(Commitlog.PbStartOnlineMigrationRequest request,
      MethodCallback<Commitlog.PbStartOnlineMigrationResponse> callback) {

  }
}
