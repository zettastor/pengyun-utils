/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
