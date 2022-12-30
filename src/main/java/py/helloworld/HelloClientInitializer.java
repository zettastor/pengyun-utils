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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.bytes.ByteArrayEncoder;

/**
 * xx.
 */
public class HelloClientInitializer extends ChannelInitializer<SocketChannel> {

  private boolean sendBytes = true;

  public HelloClientInitializer(boolean sendBytes) {
    this.sendBytes = sendBytes;
  }

  @Override
  protected void initChannel(SocketChannel ch) throws Exception {
    ChannelPipeline pipeline = ch.pipeline();

    pipeline.addLast("decoder", new ClientDecoder()); //in

    if (sendBytes) {
      pipeline.addLast("encoder", new ByteArrayEncoder()); //out
    } else {
      pipeline.addLast("forPerformance", new HelloClientOutBoundHandler()); //out
    }

    // 客户端的逻辑
    pipeline.addLast("handler", new HelloClientHandler()); //in
  }
}