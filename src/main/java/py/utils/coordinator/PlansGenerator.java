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

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class PlansGenerator {

  private static final Logger logger = LoggerFactory.getLogger(PlansGenerator.class);

  private static final int DEFAULT_MAX_RANGE_SIZE = 128 * 1024; // 128K
  private static final int DEFAULT_MAX_RANGE_SECOTR_COUNT =
      DEFAULT_MAX_RANGE_SIZE / DataBuilder.SECTOR_SIZE;

  private final long capacity;
  private final long dataSize;
  private final int maxRangeCountPerGenerate;
  private long firstPosition;
  private long generateSize;

  /**
   * xx.
   */
  public PlansGenerator(long capacity, long dataSize, int maxRangeCountPerGenerate) {
    Validate.isTrue(capacity >= dataSize, "capacity=" + capacity + ", data size=" + dataSize);
    this.firstPosition = Math.abs(new Random().nextLong()) % capacity;
    firstPosition = firstPosition / DataBuilder.SECTOR_SIZE * DataBuilder.SECTOR_SIZE;
    if (firstPosition + dataSize > capacity) {
      firstPosition = capacity - dataSize;
    }

    this.capacity = capacity;
    this.dataSize = dataSize;
    this.generateSize = 0;
    this.maxRangeCountPerGenerate = maxRangeCountPerGenerate;
  }

  /**
   * xx.
   */
  public boolean isOver() {
    if (firstPosition >= capacity) {
      logger.warn("generator={} is over 1", this.toString());
      return true;
    }

    if (generateSize >= dataSize) {
      logger.warn("generator={} is over 2", this.toString());
      return true;
    } else {
      return false;
    }
  }

  /**
   * xx.
   */
  public List<Range<Long>> generate() {
    List<Range<Long>> ranges = new ArrayList<Range<Long>>();
    logger.warn("begin to generate range, this={}", this.toString());
    Random randomForPos1 = new Random(System.currentTimeMillis());
    Random randomForPos2 = new Random(System.currentTimeMillis() + 1000);

    long rangeCount = (capacity - firstPosition) / DEFAULT_MAX_RANGE_SIZE;
    rangeCount = (rangeCount <= maxRangeCountPerGenerate ? rangeCount : maxRangeCountPerGenerate);

    while (rangeCount > 0 && !isOver()) {
      rangeCount--;
      long offset1 = randomForPos1.nextInt(DEFAULT_MAX_RANGE_SECOTR_COUNT);
      long offset2 = randomForPos2.nextInt(DEFAULT_MAX_RANGE_SECOTR_COUNT);
      Range<Long> range = null;
      if (offset1 < offset2) {
        range = Range.closedOpen(firstPosition + offset1 * DataBuilder.SECTOR_SIZE,
            firstPosition + offset2 * DataBuilder.SECTOR_SIZE);
      } else if (offset1 == offset2) {
        range = Range.closedOpen(firstPosition + offset1 * DataBuilder.SECTOR_SIZE,
            firstPosition + (offset1 + 1) * DataBuilder.SECTOR_SIZE);
      } else {
        range = Range.closedOpen(firstPosition + offset2 * DataBuilder.SECTOR_SIZE,
            firstPosition + offset1 * DataBuilder.SECTOR_SIZE);
      }

      logger.debug("generate a new range={}", range);
      ranges.add(range);
      firstPosition += DEFAULT_MAX_RANGE_SIZE;
      generateSize += (range.upperEndpoint() - range.lowerEndpoint());
    }

    logger.warn("end to generate range, range count={}, this={}", rangeCount, this.toString());
    return ranges;
  }

  @Override
  public String toString() {
    return "PlansGenerator [capacity=" + capacity + ", dataSize=" + dataSize + ", firstPosition="
        + firstPosition
        + ", generateSize=" + generateSize + "]";
  }

}
