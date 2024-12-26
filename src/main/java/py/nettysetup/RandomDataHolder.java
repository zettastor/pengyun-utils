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
