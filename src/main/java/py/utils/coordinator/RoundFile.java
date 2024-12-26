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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class RoundFile {

  private static final Logger logger = LoggerFactory.getLogger(RoundFile.class);
  @JsonIgnore
  private final List<DataWrapper> dataWrappers;
  private String filePath;
  private int snapshotId;

  @JsonIgnore
  private BufferedWriter writer;

  /**
   * xx.
   */
  public RoundFile(@JsonProperty("filePath") String filePath,
      @JsonProperty("snapshotId") int snapshotId) {
    this.filePath = filePath;
    this.dataWrappers = new ArrayList<DataWrapper>();
    this.snapshotId = snapshotId;
  }

  /**
   * xx.
   */
  public boolean exist() {
    File file = new File(filePath);
    if (file.exists()) {
      logger.warn("existing file={}", file);
      return true;
    } else {
      return false;
    }
  }

  /**
   * xx.
   */
  public void create() throws IOException {
    File file = new File(filePath);
    if (file.exists()) {
      logger.warn("existing file={}", file);
      file.delete();
    }

    logger.warn("create a new round file={}", filePath);
    file.createNewFile();
    if (writer == null) {
      writer = new BufferedWriter(new FileWriter(file, true));
    }
  }

  /**
   * xx.
   */
  public void flush() throws IOException {
    logger.warn("flush data to file={}", filePath);
    if (!exist()) {
      return;
    }

    for (DataWrapper dataWrapper : dataWrappers) {
      writer.write(dataWrapper.toJsonString());
      writer.newLine();
    }

    writer.flush();
    dataWrappers.clear();
  }

  public void add(DataWrapper dataWrapper) {
    dataWrappers.add(dataWrapper);
  }

  public List<DataWrapper> get() {
    return dataWrappers;
  }

  /**
   * xx.
   */
  public void load() throws IOException {
    logger.warn("load data from file={}", filePath);
    BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        DataWrapper dataWrapper = DataWrapper.fromJsonString(line);
        Validate.isTrue(dataWrapper.getSnapshotId() == snapshotId);
        dataWrappers.add(dataWrapper);
      }
    } finally {
      reader.close();
    }
  }

  /**
   * xx.
   */
  public void close() throws IOException {
    if (writer != null) {
      writer.close();
    }
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }

  @Override
  public String toString() {
    return "RoundFile [filePath=" + filePath + ", dataWrappers=" + dataWrappers.size()
        + ", snapshotId="
        + snapshotId + "]";
  }
}
