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
