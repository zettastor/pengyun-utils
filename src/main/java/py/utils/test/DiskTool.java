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

package py.utils.test;

import com.google.common.collect.Range;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * xx.
 */
public class DiskTool {

  private static long fileSize = 0;
  private static File file = null;

  private static volatile Map<Long, Long> mTagSet = new ConcurrentHashMap<Long, Long>();
  private static long offsetOfReadVolume = 0;
  private static long offsetOfWriteVolume = 0;
  private static long sizeOfVolume = (long) (fileSize / 512);
  // this means the loop time of checking the device or file
  private static int LoopTimes = 1;
  // default check range is from begin to end, otherwise is 
  // just check the 200M space of begining and end
  private static long checkTotalLength = (long) (400 * 1024 * 1024 / 512);
  private static long checkEachMaxLength = (long) (200 * 1024 * 1024 / 512);
  private static boolean bJustCheckHeadandEnd = false;
  private static boolean bSkip = false;

  private static boolean exitReadFlag = false;
  private static boolean exitWriteFlag = false;
  private static boolean exitFlag = false;
  private static CountDownLatch waitLatch = null;
  private static String outputTag = "output";

  /**
   * xx.
   */
  public static void main(String[] args) throws IOException {
    log("If you just want to check the first 200M and the last 200M, " 
        + "please specify checktimes(should be bigger than 0), parameter count="
        + args.length);
    log("specify like this : java -jar disktool.jar  /dev/nbd0 1");

    file = new File(args[0]);
    if (2 == args.length) {
      LoopTimes = Integer.parseInt(args[1]);
      log("as checking times have been specified, if the target size is bigger than 400M," 
          + " only the first 200M and the last 200M would be checked");
      bJustCheckHeadandEnd = true;
    } else if (3 == args.length) {
      if (0 != outputTag.compareToIgnoreCase(args[1])) {
        log("please input like this: java -jar disktool.jar /dev/nbd0 output <sector postion>");
        return;
      }

      long offset = Long.parseLong(args[2]);
      RandomAccessFile randomAccessFile = null;
      log("output sector=" + offset + " for file=" + args[0]);
      try {
        randomAccessFile = new RandomAccessFile(file, "r");
        randomAccessFile.seek(offset * 512);
        byte[] bufferSectorRead = new byte[512];
        randomAccessFile.read(bufferSectorRead);
        ByteBuffer tmp = ByteBuffer.wrap(bufferSectorRead);
        for (int i = 0; i < 512 / 16; i++) {
          log(getTag(offset, tmp.getLong(), tmp.getLong(), i));
        }
      } finally {
        if (randomAccessFile != null) {
          randomAccessFile.close();
        }
      }
      return;
    }

    RandomAccessFile randomAccessFile = null;
    randomAccessFile = new RandomAccessFile(file, "rw");
    fileSize = randomAccessFile.length();
    randomAccessFile.close();

    sizeOfVolume = (long) (fileSize / 512);
    log("the total file size=" + fileSize + ", sector count=" + sizeOfVolume + ", loop times="
        + LoopTimes);

    DiskTool diskTool = new DiskTool();
    int times = 1;
    while (!exitFlag) {
      exitReadFlag = false;
      exitWriteFlag = false;
      offsetOfReadVolume = 0;
      offsetOfWriteVolume = 0;

      try {
        if ((sizeOfVolume > checkTotalLength) && bJustCheckHeadandEnd) {
          log("the total write and read check sector count=" + checkTotalLength
              + ", It is going to check the first sector count=" + checkEachMaxLength
              + " part of the device");
          bSkip = true;
        } else if (bJustCheckHeadandEnd) {
          log("the total write and read check sector count=" + sizeOfVolume
              + ", It is going to check the whole device as the target size is less than 400M");
        } else {
          log("the total write and read check sector count=" + sizeOfVolume
              + ", It is going to check the whole device");
        }

        log("---------------test read write task started for times=" + times
            + " ---------------------");
        diskTool.testRunTasks();
        log("---------------test read write task finished for times=" + times
            + " ---------------------");
        times++;

        if (1 == LoopTimes) {
          log("The last time of run DiskTool has been finished");
          break;
        } else if (1 < LoopTimes) {
          LoopTimes = LoopTimes - 1;
          log("There are still " + LoopTimes + " times to run DiskTool");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    try {
      randomAccessFile = new RandomAccessFile(file, "rw");
      randomAccessFile.getFD().sync();
      Thread.sleep(10000);
    } catch (Exception e) {
      log("ERROR: sync content " + e);
    } finally {
      if (randomAccessFile != null) {
        randomAccessFile.close();
      }
    }
  }

  private static void setSectorTag(long sector, long date, byte[] tag) {
    ByteBuffer buffer = ByteBuffer.wrap(tag);
    long startOffset = (sector << 9);
    for (int loop = 0; loop < 512 / 16; loop++) {
      buffer.putLong(startOffset + loop * 16);
      buffer.putLong(date);
    }
  }

  private static void log(String message) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    System.out.println("\n Thread=" + Thread.currentThread().getId() + ", time="
        + simpleDateFormat.format(new Date()) + ", message:" + message);
  }

  private static void log(String message, long threadId) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    System.out.println(
        "\n Thread=" + threadId + ", time=" + simpleDateFormat.format(new Date()) + ", message:"
            + message);
  }

  private static String formatDate(long timestamp) {
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    return simpleDateFormat.format(new Date(timestamp));
  }

  private static String getTag(long sector, long offset, long timstamp, long index) {
    long newSector = offset / 512;
    long newIndex = (offset % 512) / 16;
    return "offset=" + offset + ", timestamp=" + formatDate(timstamp) + ", index=" + newIndex
        + ", old=" + index
        + ", sector=" + newSector + ", old=" + sector;
  }

  /**
   * xx.
   */
  public void testRunTasks() throws Exception {
    waitLatch = new CountDownLatch(2);
    for (int i = 0; i < 1; i++) {
      Thread t = new Thread(new WriteTask());
      t.setName("write");
      t.start();
    }
    for (int i = 0; i < 1; i++) {
      Thread t = new Thread(new ReadTask());
      t.setName("read");
      t.start();
    }

    waitLatch.await();
    mTagSet.clear();
  }

  private synchronized Range<Long> getNextWriteRange() {

    Random rand = new Random();
    int size;

    if (offsetOfWriteVolume >= sizeOfVolume) {
      exitWriteFlag = true;
      log("set WriteFlag to exit as write offset " + offsetOfWriteVolume
          + " is no less than device size "
          + sizeOfVolume);
      return null;
    }

    if (bSkip && (offsetOfWriteVolume >= checkEachMaxLength)) {
      log("#### the next step is going to check the last" 
          + " 200M part of the device, read offset sector="
          + offsetOfReadVolume + ", write offset sector=" + offsetOfWriteVolume);
      offsetOfWriteVolume = sizeOfVolume - checkEachMaxLength;
      offsetOfReadVolume = offsetOfWriteVolume;
      bSkip = false;
      log("#### change the Write and Read offset to sector= " + offsetOfWriteVolume
          + ", sizeOfVolume="
          + sizeOfVolume);
    }

    // make sure size is 1=512
    size = rand.nextInt(512 - 1);
    size++;

    // adjust the size to avoid out of range
    if (offsetOfWriteVolume + size >= sizeOfVolume) {
      log("#### write exceed the size of volume=" + sizeOfVolume + ", size=" + size + ", offset="
          + offsetOfWriteVolume + ", new size=" + (sizeOfVolume - offsetOfWriteVolume));
      size = (int) (sizeOfVolume - offsetOfWriteVolume);
    }
    Range<Long> range = null;

    range = Range.closedOpen(offsetOfWriteVolume, offsetOfWriteVolume + size);
    offsetOfWriteVolume = (offsetOfWriteVolume + size);
    return range;
  }

  private synchronized Range<Long> getNextReadRange() {

    Random rand = new Random();
    int size;

    if (offsetOfWriteVolume == offsetOfReadVolume) {
      if (offsetOfReadVolume >= sizeOfVolume) {
        exitReadFlag = true;
        log("set ReadFlag to exit as offsetRead " + offsetOfReadVolume
            + " is no less than device size "
            + sizeOfVolume);
      }
      log("read offset: " + offsetOfReadVolume + " is equal to write offset:"
          + offsetOfWriteVolume);
      return null;
    }

    // make sure size is 1-512
    size = rand.nextInt(512 - 1);
    size++;

    if (offsetOfReadVolume + size >= offsetOfWriteVolume) {
      log("#### read exceed write offset=" + offsetOfWriteVolume + ", size=" + size + ", offset="
          + offsetOfReadVolume + ", new size=" + (offsetOfWriteVolume - offsetOfReadVolume));
      size = (int) (offsetOfWriteVolume - offsetOfReadVolume);
    }
    Range<Long> range = null;

    range = Range.closedOpen(offsetOfReadVolume, offsetOfReadVolume + size);
    offsetOfReadVolume = (offsetOfReadVolume + size);
    return range;
  }

  private class WriteTask implements Runnable {

    @Override
    public void run() {
      long threadId = Thread.currentThread().getId();
      byte[] bufferSector = new byte[512];
      RandomAccessFile randomAccessFile = null;
      log("write task begin", threadId);
      try {
        randomAccessFile = new RandomAccessFile(file, "rwd");

        while (!exitWriteFlag) {
          Range<Long> range = getNextWriteRange();
          if (range == null) {
            log("write range is null, continue to get the next range", threadId);
            continue;
          }

          long upperPoint = range.upperEndpoint();
          long lowerPoint = range.lowerEndpoint();
          long timeStamp = System.currentTimeMillis();
          log("write start sector=" + lowerPoint + ", end sector=" + upperPoint + ", total sector="
              + (upperPoint - lowerPoint), threadId);
          while (lowerPoint < upperPoint && !exitFlag) {
            setSectorTag(lowerPoint, timeStamp, bufferSector);
            long offset = lowerPoint * 512;
            randomAccessFile.seek(offset);
            randomAccessFile.write(bufferSector);
            mTagSet.put(lowerPoint, timeStamp);
            log("write sector=" + lowerPoint + ", offset=" + offset + " over", threadId);
            lowerPoint++;
          }
        }

      } catch (Exception e) {
        log("ERROR: can not write data " + e);
      } finally {
        if (randomAccessFile != null) {
          try {
            randomAccessFile.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
      waitLatch.countDown();
      log("write task finish", threadId);
    }
  }

  private class ReadTask implements Runnable {

    @Override
    public void run() {
      long threadId = Thread.currentThread().getId();
      log("read task begine", threadId);

      byte[] bufferSectorRead = new byte[512];
      byte[] bufferSectorWrite = new byte[512];
      RandomAccessFile randomAccessFile = null;
      try {
        randomAccessFile = new RandomAccessFile(file, "r");
        while (!exitReadFlag) {
          Range<Long> range = getNextReadRange();
          if (range == null) {
            log("read range is null, wait a second to get the next range", threadId);
            Thread.sleep(1000);
            continue;
          }

          long upperPoint = range.upperEndpoint();
          long lowerPoint = range.lowerEndpoint();
          Long timestamp = null;
          log("read start sector=" + lowerPoint + ", end sector=" + upperPoint + ", total sector="
              + (upperPoint - lowerPoint), threadId);
          while ((lowerPoint < upperPoint) && !exitFlag) {

            timestamp = mTagSet.get(lowerPoint);
            if (timestamp == null) {
              Thread.sleep(100);
              continue;
            }

            mTagSet.remove(lowerPoint);

            setSectorTag(lowerPoint, timestamp.longValue(), bufferSectorWrite);
            long offset = lowerPoint * 512;
            randomAccessFile.seek(offset);
            int size = randomAccessFile.read(bufferSectorRead);
            if (size != 512) {
              log("ERROR: read data not completely, only=" + size + ", expected=" + 512);
            }
            log("read sector=" + lowerPoint + ", offset=" + offset + " over", threadId);

            if (!Arrays.equals(bufferSectorRead, bufferSectorWrite)) {
              exitReadFlag = true;
              exitWriteFlag = true;
              exitFlag = true;

              ByteBuffer src = ByteBuffer.wrap(bufferSectorRead);
              ByteBuffer des = ByteBuffer.wrap(bufferSectorWrite);
              for (int loop = 0; loop < 512 / 16; loop++) {
                long offsetSrc = src.getLong();
                long timestampSrc = src.getLong();
                long offsetDes = des.getLong();
                long timestampDes = des.getLong();

                if (offsetSrc != offsetDes || timestampDes != timestampSrc) {
                  log("ERROR: read tag:" + getTag(lowerPoint, offsetSrc, timestampSrc, loop)
                          + ", expected tag:" + getTag(lowerPoint, offsetDes, timestampDes, loop),
                      threadId);
                }
              }

              log("ERROR: data are not identical at sector=" + lowerPoint, threadId);
            }
            lowerPoint++;
          }
        }
      } catch (Exception e) {
        log("ERROR: can not read data " + e);
      } finally {
        if (randomAccessFile != null) {
          try {
            randomAccessFile.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }
      waitLatch.countDown();
      log("read task finish", threadId);
    }
  }
}
