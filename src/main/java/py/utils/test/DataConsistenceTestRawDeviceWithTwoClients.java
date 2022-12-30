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
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.DirectAlignedBufferAllocator;
import py.datanode.storage.impl.PageAlignedStorage;
import py.exception.StorageException;
import py.storage.Storage;
import py.volumetools.CreateUnit;

/**
 * xx.
 */
public class DataConsistenceTestRawDeviceWithTwoClients {
  private static final Logger logger = 
      LoggerFactory.getLogger(DataConsistenceTestRawDeviceWithTwoClients.class);

  public static Byte firstClientByte = '1';
  public static Byte secondClientByte = '2';
  public static boolean singleThread = false;
  static ConcurrentLinkedQueue<Range<Long>> firstPlan = null;
  static ConcurrentLinkedQueue<Range<Long>> secondPlan = null;
  static ByteBuffer firstByte;
  static ByteBuffer secondByte;
  static int printCount = 0;
  static ConcurrentLinkedQueue<Range<Long>> firstPlanCheck = 
      new ConcurrentLinkedQueue<Range<Long>>();
  static ConcurrentLinkedQueue<Range<Long>> secondPlanCheck =
      new ConcurrentLinkedQueue<Range<Long>>();
  static long lastUpperEndPointInFirstClient = 0;
  static long lastUpperEndPointInSecondClient = 0;
  private static int unitLen = 512;
  private static int alignLen = 512;
  private static int blockSize = 1024 * 4;
  private static long startPos = blockSize * 1024L;

  /**
   * xx.
   */
  public static void testTwoClient(String firstFileName, String secondFileName, long storageSize,
      long waitTime,
      int threadNum, boolean singleThread) throws IOException, StorageException {

    final ReentrantLock rafLock1 = new ReentrantLock();
    final ReentrantLock rafLock2 = new ReentrantLock();

    // generate two write plan which are not touch each others' plan
    initWritePlans(storageSize);

    final Storage raf1 = new PageAlignedStorage("firstFile", new File(firstFileName));
    Storage raf2Temp = null;
    if (secondFileName != null) {
      raf2Temp = new PageAlignedStorage("secondFile", new File(secondFileName));
    } else {
      raf2Temp = raf1;
    }
    final Storage raf2 = raf2Temp;

    // multi-thread to write first client
    final CountDownLatch writeLatch = new CountDownLatch(1);
    int firstClientCount = threadNum;
    int secondClientCount = singleThread ? 0 : threadNum;

    final CountDownLatch allThreadLatch = new CountDownLatch(firstClientCount + secondClientCount);
    AtomicInteger threadCount1 = new AtomicInteger(0);
    AtomicInteger threadCount2 = new AtomicInteger(0);
    for (int i = 0; i < firstClientCount; i++) {
      Thread writeThread = new Thread() {

        public void run() {
          int myCount = threadCount1.incrementAndGet();
          try {
            writeLatch.await();
            while (true) {
              Range<Long> range = firstPlan.poll();
              if (range == null) {
                allThreadLatch.countDown();
                return;
              } else {
                firstPlanCheck.add(range);
              }
              printCount++;
              long pos = range.lowerEndpoint();
              long jumpedNum = pos - lastUpperEndPointInFirstClient;
              lastUpperEndPointInFirstClient = range.upperEndpoint();
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              try {
                rafLock1.lock();
                firstByte.clear();
                firstByte.limit(length);
                raf1.write(pos, firstByte);
                java.util.Date currentDate = new java.util.Date();

                System.out.println(currentDate.toString() + " first client[" + myCount
                    + "] has jumped:[" + jumpedNum + "], wrote at lower endpoint:["
                    + range.lowerEndpoint() + ":" + range.lowerEndpoint() / blockSize
                    + "] <==> upper endpoint:[" + range.upperEndpoint() + ":"
                    + range.upperEndpoint() / blockSize + "],  length:["
                    + (range.upperEndpoint() - range.lowerEndpoint()) + "]");
              } finally {
                rafLock1.unlock();
              }
            }
          } catch (Exception e) {
            System.out.println("failed to persist content to file" + e);
            System.exit(-1);
          }
        }
      };
      writeThread.start();
    }

    for (int i = 0; i < secondClientCount; i++) {
      Thread writeThread = new Thread() {
        public void run() {
          int myCount = threadCount2.incrementAndGet();
          try {
            writeLatch.await();
            while (true) {
              Range<Long> range = secondPlan.poll();
              if (range == null) {
                allThreadLatch.countDown();
                return;
              } else {
                secondPlanCheck.add(range);
              }
              printCount++;
              long pos = range.lowerEndpoint();
              long jumpedNum = pos - lastUpperEndPointInSecondClient;
              lastUpperEndPointInSecondClient = range.upperEndpoint();
              int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
              try {
                rafLock2.lock();
                secondByte.clear();
                secondByte.limit(length);
                raf2.write(pos, secondByte);
                System.out.println(new java.util.Date().toString() + " second client[" + myCount
                    + "] has jumped:[" + jumpedNum + "], wrote at lower endpoint:["
                    + range.lowerEndpoint() + ":" + range.lowerEndpoint() / blockSize
                    + "] <==> upper endpoint:[" + range.upperEndpoint() + ":"
                    + range.upperEndpoint() / blockSize + "],  length:["
                    + (range.upperEndpoint() - range.lowerEndpoint()) + "]");
              } finally {
                rafLock2.unlock();
              }
            }
          } catch (Exception e) {
            System.out.println("failed to persist content to file" + e);
            System.exit(-1);
          }
        }
      };
      writeThread.start();
    }

    writeLatch.countDown();
    try {
      allThreadLatch.await();
    } catch (InterruptedException e) {
      logger.error("", e);
    }


    try {
      System.out.println(
          new java.util.Date().toString() + " >>>>>>wait for data persist to disk, about "
              + waitTime + " ms ...");
      Thread.sleep(waitTime * 1000);
    } catch (InterruptedException e) {
      System.out.println("sleep caught an exception" + e);
    }

    System.out
        .println("re-open device1[" + firstFileName + "], and device2[" + secondFileName + "]");
    final Storage reopenRaf1 = raf2;
    final Storage reopenRaf2 = raf1;
    // read first client
    while (true) {
      Range<Long> range = firstPlanCheck.poll();
      if (range == null) {
        break;
      }
      printCount++;

      long pos = range.lowerEndpoint();
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
      if (pos % alignLen != 0L || length % unitLen != 0) {
        System.out
            .println("[" + range.lowerEndpoint() + ", " + range.upperEndpoint() + "]. Pos " + pos
                + " is not aligned to " + alignLen + " or length " + length + " is not times of "
                + unitLen);
        System.exit(-1);
      }

      ByteBuffer readBuf = DirectAlignedBufferAllocator.allocateAlignedByteBuffer(length);
      reopenRaf2.read(pos, readBuf);
      System.out
          .println(new java.util.Date().toString() + "first client read check, lower endpoint:["
              + range.lowerEndpoint() + "] <==> upper endpoint:[" + range.upperEndpoint()
              + "],  length:["
              + (range.upperEndpoint() - range.lowerEndpoint()) + "]");
      readBuf.clear();
      firstByte.clear();
      for (int n = 0; n < readBuf.capacity(); n++) {
        byte src = firstByte.get();
        byte dst = readBuf.get();
        if (src != dst) {
          System.out.println("first client not equal, happen at [" + (range.lowerEndpoint() + n)
              + "], between lower endpoint:[" + range.lowerEndpoint() + "] <==> upper endpoint:["
              + range.upperEndpoint() + "]");
          System.out.println("first client not equal, expect value[" + src
              + "], but actual value[" + dst + "]");
          System.exit(-1);
        }
      }
    }

    while (!singleThread) {
      Range<Long> range = secondPlanCheck.poll();
      if (range == null) {
        break;
      }
      printCount++;

      long pos = range.lowerEndpoint();
      int length = (int) (range.upperEndpoint() - range.lowerEndpoint());
      if (pos % alignLen != 0L || length % unitLen != 0) {
        System.out
            .println("[" + range.lowerEndpoint() + ", " + range.upperEndpoint() + "]. Pos " + pos
                + " is not aligned to " + alignLen + " or length " + length + " is not times of "
                + unitLen);
        System.exit(-1);
      }

      ByteBuffer readBuf = DirectAlignedBufferAllocator.allocateAlignedByteBuffer(length);
      reopenRaf1.read(pos, readBuf);
      System.out
          .println(new java.util.Date().toString() + "first client read check, lower endpoint:["
              + range.lowerEndpoint() + "] <==> upper endpoint:[" + range.upperEndpoint()
              + "],  length:["
              + (range.upperEndpoint() - range.lowerEndpoint()) + "]");
      readBuf.clear();
      secondByte.clear();
      for (int n = 0; n < readBuf.capacity(); n++) {
        byte src = secondByte.get();
        byte dst = readBuf.get();
        if (src != dst) {
          System.out.println("first client not equal, happen at [" + (range.lowerEndpoint() + n)
              + "], between lower endpoint:[" + range.lowerEndpoint() + "] <==> upper endpoint:["
              + range.upperEndpoint() + "]");
          System.out.println("first client not equal, expect value[" + src
              + "], but actual value[" + dst + "]");
          System.exit(-1);
        }
      }
    }

    reopenRaf1.close();
    reopenRaf2.close();
  }

  private static void initWritePlans(long storageSize) {
    long offset = startPos;
    // maxCount must be not larger than alignLen/unitLen
    int maxCount = 3;

    firstPlan = new ConcurrentLinkedQueue<Range<Long>>();
    secondPlan = new ConcurrentLinkedQueue<Range<Long>>();
    Random random = new Random();
    while (true) {
      int thisLoopJumpNum = random.nextInt(maxCount);
      if (thisLoopJumpNum == 0) {
        thisLoopJumpNum = 1;
      }
      thisLoopJumpNum *= alignLen;
      offset += thisLoopJumpNum;
      long thisLoopLen = random.nextInt(maxCount);
      if (thisLoopLen == 0) {
        thisLoopLen = 1;
      }
      thisLoopLen *= unitLen;
      if (offset + thisLoopLen >= storageSize) {
        break;
      }

      Range<Long> range = Range.closedOpen(offset, offset + thisLoopLen);

      // add the range to the set
      if (!singleThread && random.nextBoolean()) {
        secondPlan.add(range);
      } else {
        firstPlan.add(range);
      }
      // looking for the next aligned offset
      offset += thisLoopLen;
      offset = offset % alignLen == 0 ? offset : alignLen * (offset / alignLen + 1);
    }

    firstByte = generateAliginedByteBuffer(unitLen * maxCount, true);
    secondByte = generateAliginedByteBuffer(unitLen * maxCount, false);
  }

  private static ByteBuffer generateAliginedByteBuffer(int len, boolean isFirst) {
    ByteBuffer buffer = DirectAlignedBufferAllocator.allocateAlignedByteBuffer(len);
    byte[] arrayHavingRandomNumber = new byte[len];
    Random random = new Random();
    random.nextBytes(arrayHavingRandomNumber);
    buffer.put(arrayHavingRandomNumber);
    return buffer;
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws StorageException {
    System.out.println(
        "Note: Please use raw device to do the test, otherwise the " 
            + "file system block device buffer will impact test accuracy");

    if (args.length != 4 && args.length != 5) {
      System.out.println(
          "Usage: \n device-name device-size(bytes) wait-time(seconds) num-jobs" 
              + " Or \n device-name1 device-name2 device-size wait-time num-jobs");
      System.exit(-1);
    }

    try {
      boolean singleClient = true;
      String devName1 = null;
      String devName2 = null;
      long deviceSize = 0L;
      long timeToWaitBetweenWriteAndRead = 0L;
      int numThreads = 0;

      if (args.length == 5) {
        singleClient = false;
        devName1 = args[0];
        devName2 = args[1];
        deviceSize = Long.valueOf(args[2]);
        timeToWaitBetweenWriteAndRead = Long.valueOf(args[3]);
        numThreads = Integer.valueOf(args[4]);
      } else if (args.length == 4) {
        singleClient = true;
        devName1 = args[0];
        devName2 = null;
        deviceSize = Long.valueOf(args[1]);
        timeToWaitBetweenWriteAndRead = Long.valueOf(args[2]);
        numThreads = Integer.valueOf(args[3]);
      }

      testTwoClient(devName1, devName2, deviceSize, timeToWaitBetweenWriteAndRead, numThreads,
          singleClient);
      System.out.println("Successfully test two client, congratulations!");
    } catch (IOException e) {
      System.out.println("met error in two client test" + e);
    }
  }
}
