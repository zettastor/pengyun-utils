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

package py.memorynbdserver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.EventExecutorGroup;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.lib.StorageDriver;
import py.coordinator.nbd.NbdRequestFrameDispatcher;
import py.coordinator.nbd.NbdResponseSender;
import py.coordinator.nbd.ProtocoalConstants;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.nbd.request.NbdRequestType;
import py.coordinator.nbd.request.Reply;
import py.coordinator.nbd.request.Request;
import py.coordinator.nbd.request.RequestHeader;
import py.coordinator.workerfactory.HeartbeatWorker;
import py.coordinator.workerfactory.PydHeartbeatWorker;

public class MemoryNbdProcessor extends ChannelInboundHandlerAdapter {

  private static final Logger logger = LoggerFactory.getLogger(MemoryNbdProcessor.class);
  private final long memorySize;
  private final PydClientManager pydClientManager;
  private ByteBuffer buffer;
  private Semaphore ioDepth;
  private HeartbeatWorker heartbeatWorker;
  private EventExecutorGroup eventExecutorGroup;
  private NbdResponseSender sender;
  private StorageDriver driver;

  /**
   * xx.
   */
  public MemoryNbdProcessor(long memorySize, int ioDepth, PydClientManager pydClientManager,
      StorageDriver storageDriver) {
    this.memorySize = memorySize;
    this.ioDepth = new Semaphore(ioDepth);
    this.buffer = ByteBuffer.allocate((int) this.memorySize);
    Validate.notNull(pydClientManager);
    this.pydClientManager = pydClientManager;
    this.heartbeatWorker = new PydHeartbeatWorker(pydClientManager, storageDriver);
    sender = new NbdResponseSender();
    sender.setPydClientManager(pydClientManager);
    this.driver = storageDriver;
  }

  public void setEventExecutorGroup(EventExecutorGroup eventExecutorGroup) {
    this.eventExecutorGroup = eventExecutorGroup;
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (eventExecutorGroup != null) {
      eventExecutorGroup.execute(new Runnable() {
        @Override
        public void run() {
          try {
            process(ctx, msg);
          } catch (Exception e1) {
            logger.error("caught an exception ", e1);
            ctx.fireExceptionCaught(e1);
          }
        }
      });
    } else {
      try {
        process(ctx, msg);
      } catch (Exception e1) {
        logger.error("caught an exception ", e1);
        throw e1;
      }
    }
  }

  private void process(final ChannelHandlerContext ctx, final Object msg) {
    Collection<Request> requests = (Collection<Request>) msg;
    for (Request request : requests) {
      // if coordinator is shutting down, reply all request as EIO for now
      RequestHeader requestHeader = request.getHeader();
      NbdRequestType requestType = requestHeader.getRequestType();
      if (requestType == NbdRequestType.Read) {
        this.pydClientManager.markReadComing(ctx.channel());
        ByteBuf body = processReadRequest(requestHeader, ctx.channel());
        Reply reply = Reply
            .generateReply(request.getHeader(), ProtocoalConstants.SUCCEEDED, body, ctx.channel());
        ctx.channel().writeAndFlush(reply.asByteBuf());
        this.pydClientManager.markReadResponse(ctx.channel());
      } else if (requestType == NbdRequestType.Write) {
        logger.info("write request header:{}", requestHeader);
        this.pydClientManager.markWriteComing(ctx.channel());
        processWriteRequest(request, ctx.channel());
        Reply reply = Reply
            .generateReply(request.getHeader(), ProtocoalConstants.SUCCEEDED, null, ctx.channel());
        ctx.channel().writeAndFlush(reply.asByteBuf());
        this.pydClientManager.markWriteResponse(ctx.channel());
      } else if (requestType == NbdRequestType.Disc || requestType == NbdRequestType.Flush) {
        // current not support
        Reply reply = Reply
            .generateReply(request.getHeader(), ProtocoalConstants.SUCCEEDED, null, ctx.channel());
        ctx.channel().writeAndFlush(reply.asByteBuf());
      } else if (requestType == NbdRequestType.Heartbeat) {
        this.pydClientManager.markHeartbeat(ctx.channel());
        Reply reply = Reply.generateHeartbeatReply(request.getHeader(), ctx.channel());
        logger.info("client{} send heartbeat:{}", ctx.channel().remoteAddress(), requestHeader);
        logger.info("ReplyMagic:{} ReplyHandler:{}", reply.getResponse().getHandler(),
            reply.getResponse().getReplyMagic());
        ctx.channel().writeAndFlush(reply.asByteBuf());
      } else if (requestType == NbdRequestType.ActiveHearbeat) {
        logger.warn("Receive a ActiveHeartbeat request from remote client:{}.",
            ctx.channel().remoteAddress().toString());
        pydClientManager
            .markActiveHeartbeat(ctx.channel(), new NbdRequestFrameDispatcher(driver, sender));
      } else {
        Reply reply = Reply
            .generateReply(requestHeader, ProtocoalConstants.ENOEXEC, null, ctx.channel());
        ctx.channel().writeAndFlush(reply.asByteBuf());
      }
    }
  }

  private ByteBuf processReadRequest(RequestHeader requestHeader, Channel channel) {
    ByteBuf body;
    try {
      int length = (int) requestHeader.getLength();
      byte[] data = new byte[length];

      trySemaphore(channel);
      // get data
      synchronized (buffer) {
        int offset = (int) requestHeader.getOffset();
        Validate.isTrue(offset >= 0);
        try {
          buffer.position(offset);
          buffer.get(data);
        } catch (Exception e) {
          throw e;
        } finally {
          this.ioDepth.release();
        }
      }

      body = UnpooledByteBufAllocator.DEFAULT.buffer(length, length);
      body.markReaderIndex();
      body.writeBytes(data);
      body.resetReaderIndex();
    } finally {
      logger.info("nothing need to do here");
    }
    return body;
  }

  private void processWriteRequest(Request request, Channel channel) {
    try {
      RequestHeader requestHeader = request.getHeader();
      int offset = (int) requestHeader.getOffset();
      Validate.isTrue(memorySize >= offset + requestHeader.getLength());

      trySemaphore(channel);
      synchronized (buffer) {
        try {
          byte[] data = new byte[request.getBody().readableBytes()];
          request.getBody().readBytes(data);
          buffer.position(offset);
          buffer.put(data);
          request.getBody().release();
        } catch (Exception e) {
          throw e;
        } finally {
          this.ioDepth.release();
        }
      }
    } finally {
      logger.info("nothing need to do here");
    }

  }

  private void trySemaphore(Channel channel) {
    this.ioDepth.tryAcquire();
  }

}
