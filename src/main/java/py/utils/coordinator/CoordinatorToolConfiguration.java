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

import com.beust.jcommander.Parameter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.Validate;
import py.utils.test.ConsoleLogger;

/**
 * xx.
 */
public class CoordinatorToolConfiguration {

  public static final String VOLUME_SIZE = "-vs";
  public static final String SEGMENT_SIZE = "-ss";
  public static final String PAGE_SIZE = "-ps";
  public static final String DIH = "-dih";
  public static final String HELP = "-h";
  public static final String VOLUME_NAME = "-vn";
  public static final String STORAGE_POOL_NAME = "-spn";
  public static final String DOMAIN_NAME = "-dn";
  public static final String RUN_TIME_SECOND = "-runtime";
  public static final String ROUND_TIMES = "-roundtimes";
  public static final String WRITE_THREAD_COUNT = "-wtc";
  public static final String READ_THREAD_COUNT = "-rtc";
  public static final String PAGE_WRAPPER_COUNT = "-pwc";
  public static final String SNAPSHOT = "-snapshot";
  public static final String DATA_SIZE = "-size";
  public static final String DEBUG_LEVEL = "-level";
  public static final String READ_REPEAT_TIMES = "-rrt";
  public static final String WRITE_REPEAT_TIMES = "-wrt";
  public static final String CHECK_DATA = "-check";
  public static final String MAX_RANGE_COUNT_PER_GENERATE = "-xrcpg";
  @Parameter(names = VOLUME_SIZE, description = "volume size is multiple of segment unit size, " 
      + "default is 10 times of segment unit size", required = false)
  public long volumeSize = 10;
  @Parameter(names = SEGMENT_SIZE, description = "segment unit size, " 
      + "such as 1024M, 1G and so on", required = false)
  public String segmentSize = "1G";
  @Parameter(names = PAGE_SIZE, description = "page size, such as 4096, " 
      + "unit is byte", required = false)
  public int pageSize = 4096;
  @Parameter(names = DIH, description = "specify a <ip:port> or " 
      + "<ip> on which deloy dih service", required = false)
  public String dih = "10.0.1.101:10000";
  @Parameter(names = VOLUME_NAME, description = "volume name for test", required = false)
  public String volumeName = "VolumeNameForCopyPage";
  @Parameter(names = STORAGE_POOL_NAME, description = "storage pool name on which " 
      + "volume will create", required = false)
  public String storagePoolName = "StoragePoolNameForCopyPage";
  @Parameter(names = DOMAIN_NAME, description = "domain name on which storage " 
      + "pool will create", required = false)
  public String domainName = "DomainNameForCopyPage";
  @Parameter(names = RUN_TIME_SECOND, description = "the last time that the tool should run, " 
      + "unit: second, it will decide if the test should finish", required = false)
  public long runtimeSecond = 60 * 60 * 2;
  @Parameter(names = ROUND_TIMES, description = "the loop times for test," 
      + " it will decide if the test should finish", required = false)
  public int roundTimes = 10;
  @Parameter(names = WRITE_THREAD_COUNT, description = "thread count for writing", required = false)
  public int writeThreadCount = 1;
  @Parameter(names = READ_THREAD_COUNT, description = "thread count for reading", required = false)
  public int readThreadCount = 1;
  @Parameter(names = PAGE_WRAPPER_COUNT, description = "page wrapper count", required = false)
  public int pageWrapperCount = 512;
  @Parameter(names = HELP, description = "print all parameters detailed description", help = false)
  private boolean help;
  @Parameter(names = SNAPSHOT, description = "create a new snapshot " 
      + "after a round is over", help = false)
  private boolean snapshot;
  @Parameter(names = DATA_SIZE, description = "the data size to write, unit: MB", required = false)
  private long dataSize = 10;
  @Parameter(names = DEBUG_LEVEL, description = "set the logger level:" 
      + "debug, info, warn, error, default: debug", required = false)
  private String debugLevel = "debug";
  @Parameter(names = READ_REPEAT_TIMES, description = "read repeat times which means " 
      + "that we submit read request n times continuously", required = false)
  private int readRepeatTimes = 1;
  @Parameter(names = WRITE_REPEAT_TIMES, description = "write repeat times which means " 
      + "that we submit write request n times continuously", required = false)
  private int writeRepeatTimes = 2;
  @Parameter(names = CHECK_DATA, description = "check the data that i have written", help = false)
  private boolean check = false;
  @Parameter(names = MAX_RANGE_COUNT_PER_GENERATE, description = "for considering the coordinator " 
      + "tool heap size, you should set the max range count per generate", required = false)
  private int maxRangeCountPerGenerate = 10000;

  public int getMaxRangeCountPerGenerate() {
    return maxRangeCountPerGenerate;
  }

  public void setMaxRangeCountPerGenerate(int maxRangeCountPerGenerate) {
    this.maxRangeCountPerGenerate = maxRangeCountPerGenerate;
  }

  public boolean isCheck() {
    return check;
  }

  public void setCheck(boolean check) {
    this.check = check;
  }

  public int getReadRepeatTimes() {
    return readRepeatTimes;
  }

  public void setReadRepeatTimes(int readRepeatTimes) {
    this.readRepeatTimes = readRepeatTimes;
  }

  public int getWriteRepeatTimes() {
    return writeRepeatTimes;
  }

  public void setWriteRepeatTimes(int writeRepeatTimes) {
    this.writeRepeatTimes = writeRepeatTimes;
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
  }

  public long getDataByteSize() {
    return dataSize * 1024 * 1024;
  }

  public String getDebugLevel() {
    return debugLevel;
  }

  public void setDebugLevel(String debugLevel) {
    this.debugLevel = debugLevel;
  }

  public int getRoundTimes() {
    return roundTimes;
  }

  public void setRoundTimes(int roundTimes) {
    this.roundTimes = roundTimes;
  }

  public String getSegmentSize() {
    return segmentSize;
  }

  public void setSegmentSize(String segmentSize) {
    this.segmentSize = segmentSize;
  }

  public long getRumtimeMs() {
    return runtimeSecond * 1000;
  }

  /**
   * xx.
   */
  public long getSegmentBytesSize() {
    Pattern pattern = Pattern.compile("(\\d+)([k|K|m|M|g|G])");
    Matcher matcher = pattern.matcher(segmentSize);
    System.out.println(
        "result=" + matcher.matches() + ", results=" + matcher.toMatchResult() + ", " + matcher
            .groupCount());
    Validate.isTrue(matcher.groupCount() == 1 || matcher.groupCount() == 2);
    String value = matcher.group(1);
    String unit = "";
    if (matcher.groupCount() == 2) {
      unit = matcher.group(2);
    }

    System.out.println("value=" + value + ", unit=" + unit);
    long longValue = Long.valueOf(value);
    if (unit.isEmpty()) {
      return longValue;
    } else if (unit.compareToIgnoreCase("k") == 0) {
      return longValue * 1024;
    } else if (unit.compareToIgnoreCase("m") == 0) {
      return longValue * 1024 * 1024;
    } else if (unit.compareToIgnoreCase("g") == 0) {
      return longValue * 1024 * 1024 * 1024;
    } else {
      throw new RuntimeException();
    }
  }

  public int getPageSize() {
    return pageSize;
  }

  public void setPageSize(int pageSize) {
    this.pageSize = pageSize;
  }

  public String getDih() {
    return dih;
  }

  public void setDih(String dih) {
    this.dih = dih;
  }

  public String getDomainName() {
    return domainName;
  }

  public void setDomainName(String domainName) {
    this.domainName = domainName;
  }

  public String getStoragePoolName() {
    return storagePoolName;
  }

  public void setStoragePoolName(String storagePoolName) {
    this.storagePoolName = storagePoolName;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public boolean isHelp() {
    return help;
  }

  public void setHelp(boolean help) {
    this.help = help;
  }

  public long getVolumeSize() {
    return volumeSize * getSegmentBytesSize();
  }

  /**
   * xx.
   */
  public void validate() {
    if (!(roundTimes > 0 && roundTimes <= Integer.MAX_VALUE)) {
      ConsoleLogger
          .log("round=" + roundTimes + ", large than 0 and less than " + Integer.MAX_VALUE);
      throw new RuntimeException("");
    }

    if (writeThreadCount <= 0) {
      ConsoleLogger.log("write thread count=" + writeThreadCount + " is not right");
    }

    if (readThreadCount <= 0) {
      ConsoleLogger.log("read thread count=" + readThreadCount + " is not right");
    }
  }

  public int getWriteThreadCount() {
    return writeThreadCount;
  }

  public void setWriteThreadCount(int writeThreadCount) {
    this.writeThreadCount = writeThreadCount;
  }

  public int getReadThreadCount() {
    return readThreadCount;
  }

  public void setReadThreadCount(int readThreadCount) {
    this.readThreadCount = readThreadCount;
  }

  public int getPageWrapperCount() {
    return pageWrapperCount;
  }

  public void setPageWrapperCount(int pageWrapperCount) {
    this.pageWrapperCount = pageWrapperCount;
  }

  public boolean isSnapshot() {
    return snapshot;
  }

  public void setSnapshot(boolean snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  public String toString() {
    return "CoordinatorToolConfiguration [volumeSize=" + volumeSize + ", segmentSize=" + segmentSize
        + ", pageSize="
        + pageSize + ", dih=" + dih + ", help=" + help + ", volumeName=" + volumeName
        + ", storagePoolName="
        + storagePoolName + ", domainName=" + domainName + ", runtimeSecond=" + runtimeSecond
        + ", roundTimes="
        + roundTimes + ", writeThreadCount=" + writeThreadCount + ", readThreadCount="
        + readThreadCount
        + ", pageWrapperCount=" + pageWrapperCount + ", snapshot=" + snapshot + ", dataSize="
        + dataSize
        + ", debugLevel=" + debugLevel + ", readRepeatTimes=" + readRepeatTimes
        + ", writeRepeatTimes="
        + writeRepeatTimes + ", check=" + check + ", maxRangeCountPerGenerate="
        + maxRangeCountPerGenerate
        + "]";
  }

}
