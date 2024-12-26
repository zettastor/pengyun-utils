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

package py.utils.test;

import java.io.File;
import java.io.IOException;

/**
 * 文件操作工具.
 */
public class FileOperationUtils {

  /**
   * xx.
   */
  public static boolean deleteFileOrFolder(String spath) {
    File file = new File(spath);
    // 判断目录或文件是否存在
    if (!file.exists()) {
      return true;
    } else {
      // 判断是否为文件
      if (file.isFile()) {  // 为文件时
        return deleteFile(spath);
      } else {  // 为目录时
        return deleteDirectory(spath);
      }
    }
  }

  /**
   * xx.
   */
  public static boolean deleteFile(String spath) {
    File file = new File(spath);
    if (!file.exists()) {
      return true;
    }
    // 路径为文件且不为空则进行删除
    if (file.isFile()) {
      return file.delete();
    }
    return false;
  }

  /**
   * xx.
   */
  public static boolean deleteDirectory(String spath) {
    return deleteDirectory(spath, true);
  }

  /**
   * xx.
   */
  public static boolean deleteDirectory(String spath, boolean delSub) {
    File dirFile = new File(spath);
    if (!dirFile.exists()) {
      return true;
    }
    //如果dir对应的文件不存在，或者不是一个目录，则退出
    if (!dirFile.isDirectory()) {
      return false;
    }
    boolean delok = true;
    //删除文件夹下的所有文件(包括子目录)
    File[] files = dirFile.listFiles();
    if (files == null || (!delSub && files.length != 0)) {
      return false;
    }

    for (File file : files) {
      //删除子文件
      if (file.isFile()) {
        delok = deleteFile(file.getAbsolutePath());
      } else {
        //删除子目录
        delok = deleteDirectory(file.getAbsolutePath(), delSub);
      }

      if (!delok) {
        return false;
      }
    }

    //删除当前目录
    return dirFile.delete();
  }

  /**
   * 创建文件夹.
   */
  public static boolean mkdirFolder(File file) {
    if (file.exists()) {
      return true;
    } else {
      return file.mkdir();
    }
  }

  /**
   * 创建文件路径.
   */
  public static boolean createFile(File file) throws IOException {
    if (file.exists()) {
      return true;
    } else {
      return file.createNewFile();
    }
  }
}
