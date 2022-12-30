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
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import py.coordinator.nbd.PydClientManager;
import py.coordinator.nbd.request.MagicType;
import py.coordinator.nbd.request.Negotiation;
import py.informationcenter.AccessPermissionType;

public class MemoryNbdConnectionHandler extends ChannelInboundHandlerAdapter {

  private final long memorySize;

  private PydClientManager pydClientManager;

  public MemoryNbdConnectionHandler(long memorySize) {
    this.memorySize = memorySize;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    int bufferSize = Negotiation.getNegotiateLength();
    ByteBuf byteBuf = Unpooled.buffer(bufferSize);
    Negotiation negotiation = MagicType.generateNegotiation(memorySize);
    negotiation.writeTo(byteBuf);
    ctx.channel().writeAndFlush(byteBuf);
    this.pydClientManager.clientActive(ctx.channel(), AccessPermissionType.READWRITE);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    this.pydClientManager.clientInactive(ctx.channel());
  }

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

}
