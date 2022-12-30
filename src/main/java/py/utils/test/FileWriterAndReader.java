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
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a test utility for data consistent check.
 *
 * <p>E.g In OPENSTACK integration test environment,
 * write data to a volume and verify data consistent from its cloned volume by this tool.
 */
public class FileWriterAndReader {

  private static final PrintStream out = System.out;

  private static final String SIZE_PATTERN = "(\\d+)(.*)";
  private boolean isWriteMode = true;
  private long size;
  private String path;
  private int blockSize = 1024 * 1024;

  /**
   * xx.
   */
  public FileWriterAndReader(boolean isWriteMode, long size, String path) {
    super();
    this.isWriteMode = isWriteMode;
    this.size = size;
    this.path = path;
  }

  /**
   * xx.
   */
  public static void printUsage() {
    out.println("Usage:");
    out.println(
        "\tjava FileWriterAndReader [file path] [read|write] [size(b|kb|Kb|mb|Mb|gb|Gb|tb|Tb)]");
  }

  /**
   * xx.
   */
  public static long parseSize(String sizeStr) throws IllegalArgumentException {
    Pattern pattern = Pattern.compile(SIZE_PATTERN);
    Matcher matcher = pattern.matcher(sizeStr);
    String unit = "";
    long size;

    if (matcher.find()) {
      size = Long.valueOf(matcher.group(1));
      unit = matcher.group(2);
      if (unit == null || unit.isEmpty()) {
        return size;
      }

      unit = unit.toLowerCase();
      if (unit.equals("b")) {
        return size;
      } else if (unit.equals("kb")) {
        return size << 10;
      } else if (unit.equals("mb")) {
        return size << 20;
      } else if (unit.equals("gb")) {
        return size << 30;
      } else if (unit.equals("tb")) {
        return size << 40;
      }
    }

    out.println("Illegal size format!");
    printUsage();
    throw new IllegalArgumentException();
  }

  /**
   * xx.
   */
  public static boolean isWrite(String mode) throws IllegalArgumentException {
    mode = mode.toLowerCase();
    if (mode.equals("read")) {
      return false;
    } else if (mode.equals("write")) {
      return true;
    } else {
      out.print("Illegal mode format! Mode: " + mode);
      printUsage();
      throw new IllegalArgumentException();
    }
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      out.println("Too short arguments!");
      printUsage();
      exit(1);
    }

    String path = args[0];
    String mode = args[1];
    String sizeStr = args[2];

    long size = 0;
    boolean isWrite = false;

    try {
      size = parseSize(sizeStr);
      isWrite = isWrite(mode);
    } catch (IllegalArgumentException e) {
      exit(1);
    }

    FileWriterAndReader fwar;

    fwar = new FileWriterAndReader(isWrite, size, path);
    fwar.execute();
  }

  public static void exit(int exitCode) {
    System.exit(exitCode);
  }

  /**
   * xx.
   */
  public void execute() throws Exception {
    long offset = 0;
    long nblocks = size / blockSize;
    byte[] data;
    byte[] readData = new byte[blockSize];

    File file = new File(path);
    RandomAccessFile raf = new RandomAccessFile(file, "rw");

    try {
      for (long i = 0; i < nblocks; i++) {
        data = generateDataAssociatedWithOffset(offset, blockSize);

        if (isWriteMode) {
          raf.write(data);
        } else {
          raf.read(readData);
          if (!Arrays.equals(data, readData)) {
            out.println("Data inconsistant! Offset: " + offset + ", Len: " + blockSize);
            exit(1);
          }
        }
        offset += data.length;
      }

      raf.getFD().sync();
    } finally {
      raf.close();
    }
  }

  private byte[] generateDataAssociatedWithOffset(long offset, int length) {
    if (length % Long.BYTES != 0) {
      out.println("Length is not aligned! Length: " + length);
      exit(1);
    }

    int times = length / Long.BYTES;
    ByteBuffer dataBuf = ByteBuffer.allocate(length);

    for (int i = 0; i < times; i++) {
      dataBuf.putLong(offset);
      offset += Long.BYTES;
    }

    return dataBuf.array();
  }
}
