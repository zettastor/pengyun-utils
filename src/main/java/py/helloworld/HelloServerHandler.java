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

package py.helloworld;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class HelloServerHandler extends SimpleChannelInboundHandler<byte[]> {

  private static final Logger logger = LoggerFactory.getLogger(HelloServerHandler.class);
  private static final int RESPONSE_LENGTH = 1;
  private static final byte[] back = new byte[RESPONSE_LENGTH];
  private AtomicLong receiveLen = new AtomicLong(0);
  private int packageSize;

  public HelloServerHandler(int packageSize) {
    this.packageSize = packageSize;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) throws Exception {
    receiveLen.addAndGet(msg.length);
    for (; ; ) {
      if (receiveLen.get() < packageSize) {
        break;
      }
      ctx.channel().writeAndFlush(back);
      receiveLen.addAndGet(-packageSize);
    }
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    logger.warn(">>>>>>>>>Remote Address:[{}] connected.", ctx.channel().remoteAddress());
    super.channelActive(ctx);
  }
}