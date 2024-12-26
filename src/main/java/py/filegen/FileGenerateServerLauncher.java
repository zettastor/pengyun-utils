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

package py.filegen;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class FileGenerateServerLauncher {

  private static final Logger logger = LoggerFactory.getLogger(FileGenerateServerLauncher.class);
  private static AtomicInteger count = new AtomicInteger(0);

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      usage();
      System.exit(-1);
    }
    String path = args[0];
    long size = getByteSize(args[1]);
    String suffix = args[2];

    writeData(path, size, suffix);

  }

  /**
   * xx.
   */
  public static void writeData(String path, long size, String suffix) {
    File file = createFile(path, suffix);
    String absolutePath = file.getAbsolutePath();
    System.out.println(
        "now is writing data to " + absolutePath.substring(0, absolutePath.lastIndexOf("/"))
            + " ...");

    long length = 1024 * 1024;
    if (size < length) {
      length = size;
    }

    StringBuilder sb = new StringBuilder();
    while (sb.length() < length) {
      sb.append(UUID.randomUUID() + " count: " + count.incrementAndGet() + "\n");
    }
    while (true) {
      if (FileUtils.sizeOf(file) < size) {
        try {
          FileUtils.write(file, sb.toString(), "UTF-8", true);
          Thread.sleep(20);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else {
        file = createFile(path, suffix);
      }
    }
  }

  /**
   * xx.
   */
  public static File createFile(String path, String suffix) {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmssSSS"); //设置日期格式
    String timeStamp = df.format(new Date());
    String fileName = path + "/" + timeStamp + "-" + suffix + ".txt";
    File file = new File(fileName);
    try {
      FileUtils.touch(file);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return file;
  }

  /**
   * xx.
   */
  public static long getByteSize(String size) {
    Pattern pattern = Pattern.compile("(\\d+)([k|K|m|M|g|G]|)");
    Matcher matcher = pattern.matcher(size);
    Validate.isTrue(matcher.matches());
    Validate.isTrue(matcher.groupCount() == 1 || matcher.groupCount() == 2);
    String value = matcher.group(1);
    String unit = "";
    if (matcher.groupCount() == 2) {
      unit = matcher.group(2);
    }

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

  private static void usage() {
    String usageInfo = "usage: java -jar FileGenerateServerLauncher.jar path fileSize suffix\n"
        + "param:path [such as: /tmp/test, ...]\n" + "param:fileSize [such as: 100M, 1G, ...]\n"
        + "param:suffix [such as: host1, host2, ...]";
    System.out.println(usageInfo);
  }
}
