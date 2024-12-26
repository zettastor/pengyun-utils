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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.coordinator.nbd.PydClientManager;

public class MemNbdIdleStateHandler extends ChannelDuplexHandler {

  private static final Logger logger = LoggerFactory.getLogger(MemNbdIdleStateHandler.class);

  private PydClientManager pydClientManager;

  public void setPydClientManager(PydClientManager pydClientManager) {
    this.pydClientManager = pydClientManager;
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      boolean close = false;
      if (e.state() == IdleState.READER_IDLE) {
        logger.warn("Channel reader:{} idle", ctx.channel().remoteAddress());
        close = true;
      } else if (e.state() == IdleState.WRITER_IDLE) {
        logger.warn("Channel writer:{} idle", ctx.channel().remoteAddress());
        close = true;
      } else if (e.state() == IdleState.ALL_IDLE) {
        logger.warn("Channel all:{} idle", ctx.channel().remoteAddress());
        close = true;
      }
      if (close) {
        boolean check = this.pydClientManager.checkAndCloseClientConnection(ctx.channel());
        if (check) {
          ctx.close();
        }
      }
    }
  }
}
