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
