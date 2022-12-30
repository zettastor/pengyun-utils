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
import org.apache.commons.lang.math.RandomUtils;
import py.netty.memory.PooledByteBufAllocatorWrapper;

/**
 * xx.
 */
public class RandomDataHolder {

  private final ByteBuf[] byteBufDatas;

  /**
   * xx.
   */
  public RandomDataHolder(int selectRange, int size) {
    byteBufDatas = new ByteBuf[selectRange];

    for (int i = 0; i < selectRange; i++) {
      ByteBuf buf = PooledByteBufAllocatorWrapper.INSTANCE.buffer(size);
      byteBufDatas[i] = buf;

      for (int j = 0; j < size; j++) {
        byte b = (byte) (RandomUtils.nextInt() & 0xFF);

        buf.writeByte(b);
      }
    }
  }

  public ByteBuf randomByteBufData() {
    return byteBufDatas[RandomUtils.nextInt(byteBufDatas.length)].retainedDuplicate();
  }
}
