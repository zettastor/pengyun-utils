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

import static py.utils.stream.writer.StreamWriter.Mode.READ;
import static py.utils.stream.writer.StreamWriter.Mode.WRITE;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamWriter {

  private static String filePath;
  private static boolean inputError = false;

  // for write
  private static int interval;
  private static int threadNum = 1;
  private static int writeCount = 0;
  private static int discardCount = 0;
  private static long writeKBPerSecond = 512;
  private static volatile boolean stopSignal = false;

  // for read
  private static int lostCount = 0;
  private static int readCount = 0;

  // for sync
  private static AtomicInteger syncCounter = new AtomicInteger(0);
  private static int syncRound;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {

    Mode mode = null;

    try {
      filePath = args[0];
      if (args.length == 1) {
        mode = READ;
      } else {
        mode = WRITE;
        interval = Integer.parseInt(args[1]);
        if (args.length == 3) {
          //                threadNum = Integer.parseInt(args[2]);
          writeKBPerSecond = Long.parseLong(args[2]);
        }
      }
    } catch (Throwable e) {
      inputError = true;
      e.printStackTrace();
      printUsage();
    }

    switch (mode) {
      case READ:
        read();
        break;
      case WRITE:
        write();
        break;
      default:
    }
  }

  private static void read() throws Exception {
    File inputFile = new File(filePath);
    RandomAccessFile raf = new RandomAccessFile(inputFile, "r");

    Runtime.getRuntime().addShutdownHook(new Thread(StreamWriter::printReadResult));

    int lastId = -1;
    while (true) {
      try {
        Data data = DataEvent.readFrom(raf);
        int myId = (int) data.getDataId();
        if (myId - lastId != 1) {
          int lost = myId - lastId - 1;
          System.out.println("[ERROR] lost " + lost + " items !");
          lostCount += lost;
        }
        lastId = myId;
        readCount++;
        System.out.println("[INFO] " + data.toString());
      } catch (Exception e) {
        System.out.println("[INFO] " + " no more things to read ");
        break;
      }
    }
  }

  private static void write() throws Exception {

    DataEvent initEvent = new DataEvent();
    initEvent.init(0, 512, System.currentTimeMillis());
    initEvent.getArray();

    final File outputFile = new File(filePath);
    final RandomAccessFile raf = new RandomAccessFile(outputFile, "rw");

    Runtime.getRuntime().addShutdownHook(new Thread(StreamWriter::printWriteResult));

    syncRound = 1000 / interval;
    if (syncRound < 1) {
      syncRound = 1;
    }

    syncRound = 1;
    int bufferSize;
    for (int i = 0; true; i++) {
      bufferSize = (int) Math.pow(2, i);
      if (bufferSize >= syncRound) {
        break;
      }
    }

    Disruptor<DataEvent> disruptor = new Disruptor<>(DataEvent::new, bufferSize,
        new ThreadFactory() {
          int threadNum = 0;

          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("consumer-" + threadNum++);
            return t;
          }
        });

    //        WriteEventWorkHandler[] workHandlers = new WriteEventWorkHandler[threadNum];
    //        for (int i = 0; i < threadNum; i++) {
    //            workHandlers[i] = new WriteEventWorkHandler(out, true);
    //        }
    //        disruptor.handleEventsWithWorkerPool(workHandlers);

    disruptor.handleEventsWith(new WriteEventHandler(raf, true));

    disruptor.start();

    RingBuffer<DataEvent> ringBuffer = disruptor.getRingBuffer();

    int eventId = 0;
    long counterWeight = writeKBPerSecond * 1024 * interval / 1000;
    while (!stopSignal) {
      try {
        long sequence = ringBuffer.tryNext();
        DataEvent writeEvent = ringBuffer.get(sequence);
        writeEvent.init(eventId++, (int) counterWeight, System.currentTimeMillis());
        ringBuffer.publish(sequence);
        System.out.println("written : " + writeEvent);
        writeCount++;
      } catch (InsufficientCapacityException e) {
        DataEvent discardEvent = new DataEvent();
        discardEvent.init(eventId++, (int) counterWeight, System.currentTimeMillis());
        System.out.println("discarded " + discardEvent);
        discardCount++;
      }
      Thread.sleep(interval);
    }
  }

  private static void printReadResult() {
    if (!inputError) {
      System.out
          .println("\n\nread result : read: " + readCount + "; lost: " + lostCount + " items");
      System.out.println("file: " + filePath);
    }
  }

  private static void printWriteResult() {
    if (!inputError) {
      stopSignal = true;
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      System.out.println(
          "\n\nwrite info : interval: " + interval + "; speed: " + writeKBPerSecond + " KB/s");
      System.out.println("file: " + filePath);
      System.out.println("result: ");

      System.out.println(
          "\twritten     count:" + writeCount + ", time:"
              + ((double) (writeCount * interval)) / 1000 + " s");
      System.out.println(
          "\tdiscarded   count:" + discardCount + ", time:"
              + ((double) (discardCount * interval)) / 1000
              + " s");
    }
  }

  private static void printUsage() {
    System.out.println("\nUsage : ");
    System.out.println("\nparameters : filePath [writeIntervalMills] [writeSpeedKB]");
    System.exit(-1);
  }

  enum Mode {
    READ, WRITE
  }

  static class WriteEventWorkHandler implements WorkHandler<DataEvent> {

    private final FileOutputStream fileOutputStream;
    private final boolean flush;

    WriteEventWorkHandler(FileOutputStream fileOutputStream, boolean flush) {
      this.fileOutputStream = fileOutputStream;
      this.flush = flush;
    }

    @Override
    public void onEvent(DataEvent writeEvent) throws IOException {
      fileOutputStream.write(writeEvent.getArray());
      if (flush && syncCounter.incrementAndGet() % syncRound == 0) {
        fileOutputStream.getFD().sync();
      }
    }
  }

  static class WriteEventHandler implements EventHandler<DataEvent> {

    private final RandomAccessFile raf;
    private final boolean flush;

    WriteEventHandler(RandomAccessFile raf, boolean flush) {
      this.raf = raf;
      this.flush = flush;
    }

    @Override
    public void onEvent(DataEvent writeEvent, long l, boolean b) throws Exception {
      try {
        raf.write(writeEvent.getArray());
      } catch (IOException e) {
        if (e.toString().toLowerCase().contains("space")) {
          System.out.println("no space left, start over");
          raf.seek(0);
        }
      }

      if (flush && syncCounter.incrementAndGet() % syncRound == 0) {
        System.out.println("sync");
        raf.getFD().sync();
      }
    }

  }
}
