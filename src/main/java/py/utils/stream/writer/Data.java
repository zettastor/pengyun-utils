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

package py.utils.stream.writer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * data for stream writer to write.
 */
public class Data {

  static DateFormat SDF = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
  private long dataId;
  private long date;
  private String threadName;
  private int dataSize;

  public Data() {
  }

  /**
   * xx.
   */
  public Data(long dataId, long date, String threadName, int dataSize) {
    this.dataId = dataId;
    this.date = date;
    this.threadName = threadName;
    this.dataSize = dataSize;
  }

  public long getDataId() {
    return dataId;
  }

  public void setDataId(long dataId) {
    this.dataId = dataId;
  }

  public long getDate() {
    return date;
  }

  public void setDate(long date) {
    this.date = date;
  }

  public String getThreadName() {
    return threadName;
  }

  public void setThreadName(String threadName) {
    this.threadName = threadName;
  }

  public int getDataSize() {
    return dataSize;
  }

  public void setDataSize(int dataSize) {
    this.dataSize = dataSize;
  }

  @Override
  public String toString() {
    return "Data{" + "dataId=" + dataId + ", date='" + SDF.format(new Date(date)) + '\''
        + ", threadName='"
        + threadName + '\'' + ", dataSize=" + dataSize + '}';
  }
}
