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

package py.utils.coordinator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import py.utils.test.ConsoleLogger;

/**
 * xx.
 */
public class DataWrapper {

  private String time;
  private long offsetSector;
  private long lengthSector;
  private int snapshotId;

  public DataWrapper() {

  }

  /**
   * xx.
   */
  public DataWrapper(long offsetSector, long lengthSector, int snapshotId, long time) {
    this.setTime(ConsoleLogger.longTimeToString(time));
    this.setOffsetSector(offsetSector);
    this.setLengthSector(lengthSector);
    this.setSnapshotId(snapshotId);
  }

  public static DataWrapper fromJsonString(String jsonString)
      throws JsonParseException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(jsonString, DataWrapper.class);
  }

  public String toJsonString() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }

  public String getTime() {
    return time;
  }

  public void setTime(long time) {
    this.time = ConsoleLogger.longTimeToString(time);
  }

  public void setTime(String time) {
    this.time = time;
  }

  public long getLongTime() throws Exception {
    return ConsoleLogger.stringToLongTime(time);
  }

  public long getOffsetSector() {
    return offsetSector;
  }

  public void setOffsetSector(long offsetSector) {
    this.offsetSector = offsetSector;
  }

  public long getLengthSector() {
    return lengthSector;
  }

  public void setLengthSector(long lengthSector) {
    this.lengthSector = lengthSector;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  /**
   * xx.
   */
  public List<byte[]> getData(boolean isSpliteBySector) throws ParseException {
    List<byte[]> datas = new ArrayList<byte[]>();
    long longTime = ConsoleLogger.stringToLongTime(time);
    if (isSpliteBySector) {
      for (int i = 0; i < lengthSector; i++) {
        DataBuilder builder = new DataBuilder(offsetSector + i, 1, longTime);
        datas.add(builder.build());
      }
    } else {
      DataBuilder builder = new DataBuilder(offsetSector, (int) lengthSector, longTime);
      datas.add(builder.build());
    }

    return datas;
  }

  @Override
  public String toString() {
    return "DataWrapper [time=" + time + ", offsetSector=" + offsetSector + ", lengthSector="
        + lengthSector
        + ", snapshotId=" + snapshotId + "]";
  }
}
