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

package py.utils.coordinator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.utils.test.ConsoleLogger;

/**
 * xx.
 */
public class DataBuilder {

  public static final int SECTOR_SIZE = 512;
  public static final int WRITE_BYTES = 16;
  private static final Logger logger = LoggerFactory.getLogger(DataBuilder.class);
  private final long offsetSector;
  private final int sectorCount;
  private final long time;

  /**
   * xx.
   */
  public DataBuilder(long offsetSector, int sectorCount, long time) {
    this.offsetSector = offsetSector;
    this.sectorCount = sectorCount;
    this.time = time;
  }

  public static void canRebuild(byte[] data, long offsetSector, String time) throws Exception {
    canRebuild(data, offsetSector, ConsoleLogger.stringToLongTime(time));
  }

  /**
   * xx.
   */
  public static void canRebuild(byte[] data, long offsetSector, long time) throws Exception {
    if (data.length % SECTOR_SIZE != 0) {
      Validate.isTrue(false, "length no algind with sector, value=" + data.length);
    }

    DataBuilder builder = new DataBuilder(offsetSector, data.length / SECTOR_SIZE, time);
    byte[] expected = builder.build();
    if (!Arrays.equals(expected, data)) {
      ByteBuffer buffer = ByteBuffer.wrap(data);
      // output all sector
      for (int i = 0; i < builder.getSectorCount(); i++) {
        for (int j = 0; j < SECTOR_SIZE / WRITE_BYTES; j++) {
          long readOffset = buffer.getLong();
          long readTime = buffer.getLong();

          if (readOffset != offsetSector + i || readTime != time) {
            logger.error("mismatch, expected offset=" + (offsetSector + i) + ", time=" + time
                + ", read offset=" + readOffset + ", time=" + readTime);
          } else {
            logger.debug("expected offset=" + (offsetSector + i) + ", time=" + time);
          }
        }
      }

      throw new Exception("miss match, builder=" + builder);
    }
  }

  public long getOffsetSector() {
    return offsetSector;
  }

  public int getSectorCount() {
    return sectorCount;
  }

  public long getTime() {
    return time;
  }

  /**
   * xx.
   */
  public byte[] build() {
    byte[] data = new byte[sectorCount * SECTOR_SIZE];
    ByteBuffer buffer = ByteBuffer.wrap(data);

    for (int i = 0; i < sectorCount; i++) {
      for (int j = 0; j < SECTOR_SIZE / WRITE_BYTES; j++) {
        buffer.putLong(offsetSector + i);
        buffer.putLong(time);
      }
    }

    return data;
  }

  @Override
  public String toString() {
    return "DataBuilder [offsetSector=" + offsetSector + ", sectorCount=" + sectorCount + ", time="
        + time + "]";
  }
}
