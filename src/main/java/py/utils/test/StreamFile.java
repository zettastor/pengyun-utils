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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import py.common.struct.Pair;

public class StreamFile {

  private static BlockingQueue<byte[]> dataQueue = new LinkedBlockingQueue<>(1);
  private static FileOutputStream out;
  private static int dataLength = 0;
  private static int writeCount;
  private static int discardCount;
  private static int interval;
  private static boolean flush;
  private static String outputPath;

  private static void writeJob(boolean flush) {
    while (true) {
      try {
        byte[] value = dataQueue.take();
        out.write(value);
        if (flush) {
          out.flush();
          out.getFD().sync();
        }
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private static void printResult() {
    System.out.println(
        "\n\nwrite info : interval: " + interval + ", sync:" + flush + ", speed:" + dataLength * (
            1000
                / interval) + " Byte/s");
    System.out.println("file: " + outputPath);
    System.out.println("result: ");

    DecimalFormat df = new DecimalFormat("#0.000");
    double writeSize = ((double) (writeCount * dataLength)) / 1024 / 1024;
    double discardSize = ((double) (discardCount * dataLength)) / 1024 / 1024;

    System.out.println(
        "\twritten     count:" + writeCount + ", time:" + ((double) (writeCount * interval)) / 1000
            + " s, size: " + df.format(writeSize) + " MB");
    System.out.println(
        "\tdiscarded   count:" + discardCount + ", time:"
            + ((double) (discardCount * interval)) / 1000
            + " s, size: " + df.format(discardSize) + " MB");
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws FileNotFoundException {
    outputPath = args[0];
    File outputFile = new File(outputPath);
    interval = Integer.parseInt(args[1]);
    flush = Integer.parseInt(args[2]) > 0;
    out = new FileOutputStream(outputFile, true);

    Runtime.getRuntime().addShutdownHook(new Thread(StreamFile::printResult));

    Thread writeThread = new Thread(() -> writeJob(flush));
    writeThread.start();

    dataLength = (256 * 1024) / (1000 / interval);
    int num = 0;
    while (true) {
      try {
        Pair<String, byte[]> data = getTime(num++);
        try {
          Thread.sleep(interval);
          if (dataQueue.add(data.getSecond())) {
            writeCount++;
            System.out.print("write   size: " + data.getSecond().length / 1024 + " Bytes :" + data
                .getFirst());
          }
        } catch (Exception ignored) {
          discardCount++;
          System.out.print(
              "discard size: " + data.getSecond().length / 1024 + " Bytes :" + data.getFirst());
        }
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
  }

  private static Pair<String, byte[]> getTime(int num) throws UnsupportedEncodingException {
    DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
    String svalue =
        num + " : " + sdf.format(new Date(System.currentTimeMillis())) + System.lineSeparator();
    ByteBuffer buffer = ByteBuffer.allocate(dataLength);
    buffer.position(dataLength - svalue.length());
    buffer.put(svalue.getBytes("UTF-8"));
    return new Pair<>(svalue, buffer.array());
  }
}
