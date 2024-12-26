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

package py.utils.stream.writer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Date;

public class DataEvent {

  public static final int MAGIC = 0xCAFEBABE;

  private Data data;
  private int eventId;
  private int counterWeight;
  private long writeTime;

  /**
   * xx.
   */
  public static Data readFrom(RandomAccessFile raf) throws IOException {
    if (raf.readInt() != MAGIC) {
      throw new IOException("BAD magic");
    }
    int length = raf.readInt();
    byte[] array = new byte[length];
    raf.read(array);
    ObjectMapper objectMapper = new ObjectMapper();
    Data data = objectMapper.readValue(array, Data.class);
    int size = data.getDataSize();
    raf.skipBytes(size - length);
    return data;
  }

  /**
   * xx.
   */
  public void init(int eventId, int counterWeight, long writeTime) {
    this.eventId = eventId;
    this.counterWeight = counterWeight;
    this.writeTime = writeTime;
  }

  /**
   * xx.
   */
  public byte[] getArray() throws UnsupportedEncodingException, JsonProcessingException {
    data = new Data(eventId, writeTime, Thread.currentThread().getName(), counterWeight);
    ObjectMapper objectMapper = new ObjectMapper();
    byte[] bytes = objectMapper.writeValueAsBytes(data);
    assert (bytes.length <= counterWeight);
    ByteBuffer buffer = ByteBuffer.allocate(counterWeight + 4 + 4);
    buffer.putInt(MAGIC);
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    return buffer.array();
  }

  @Override
  public String toString() {
    return eventId + " : " + Data.SDF.format(new Date(writeTime)) + "; size=" + counterWeight;
  }
}
