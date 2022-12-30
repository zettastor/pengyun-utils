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
