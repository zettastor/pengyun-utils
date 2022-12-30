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
