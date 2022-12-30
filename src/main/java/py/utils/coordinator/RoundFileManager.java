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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * xx.
 */
public class RoundFileManager {

  private static final Logger logger = LoggerFactory.getLogger(RoundFileManager.class);

  private static final String ROUND_FILE_DIR = System.getProperty("user.dir") + "/" + "round";

  private static final String ROUND_METADATA_FILE = ROUND_FILE_DIR + "/" + "metadata";

  private List<RoundFile> roundFiles;

  private String volumeName;
  private long volumeId;
  private long accountId;
  private long storagePoolId;
  private long domainId;

  public RoundFileManager() {
    roundFiles = new ArrayList<RoundFile>();
  }

  /**
   * xx.
   */
  public static RoundFileManager load() throws IOException {
    RoundFileManager fileManager = fromFile(ROUND_METADATA_FILE);

    for (RoundFile roundFile : fileManager.getRoundFiles()) {
      if (roundFile.exist()) {
        roundFile.load();
      }
    }

    // re-build data, maybe we have re-written some block data or
    // created snapshot to shadow some block data.
    return fileManager;
  }

  public static RoundFileManager fromFile(String path)
      throws JsonParseException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new File(path), RoundFileManager.class);
  }

  /**
   * xx.
   */
  public boolean existing() {
    File file = new File(ROUND_FILE_DIR);
    if (file.exists()) {
      if (file.isDirectory() && file.listFiles().length > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * xx.
   */
  public void create(boolean removeIfExisting) throws Exception {
    File roundFileDir = new File(ROUND_FILE_DIR);

    boolean isSuccess = false;
    if (roundFileDir.exists()) {
      if (roundFileDir.isDirectory()) {
        if (removeIfExisting) {
          FileUtils.deleteDirectory(roundFileDir);
        }
        isSuccess = roundFileDir.mkdir();
      }
    } else {
      isSuccess = roundFileDir.mkdir();
    }

    if (!isSuccess) {
      throw new Exception("can not create a new file=" + ROUND_FILE_DIR);
    }
  }

  /**
   * xx.
   */
  public RoundFile generateNewRoundFile(int snapshotId) {
    RoundFile roundFile = new RoundFile(
        ROUND_FILE_DIR + "/" + "round" + roundFiles.size() + "_" + snapshotId,
        snapshotId);
    roundFiles.add(roundFile);
    logger.warn("generate a new round file={}", roundFile);
    return roundFile;
  }

  public int getSnapshotId(String filePath) {
    String[] result = Pattern.compile("_").split(filePath);
    return Integer.valueOf(result[result.length]);
  }

  /**
   * It will merge all data wrapper, you can read and check all written data.
   */
  public List<DataWrapper> generateDataWrapper() {
    List<DataWrapper> dataWrappers = new ArrayList<DataWrapper>();

    return dataWrappers;
  }

  /**
   * xx.
   */
  public void flush() throws Exception {
    FileWriter writer = new FileWriter(ROUND_METADATA_FILE, false);
    try {
      writer.write(toJsonString());
    } finally {
      writer.close();
    }

    for (RoundFile roundFile : roundFiles) {
      try {
        roundFile.flush();
      } catch (Exception e) {
        logger.warn("caught an exception", e);
      }
    }
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public long getAccountId() {
    return accountId;
  }

  public void setAccountId(long accountId) {
    this.accountId = accountId;
  }

  public long getStoragePoolId() {
    return storagePoolId;
  }

  public void setStoragePoolId(long storagePoolId) {
    this.storagePoolId = storagePoolId;
  }

  public long getDomainId() {
    return domainId;
  }

  public void setDomainId(long domainId) {
    this.domainId = domainId;
  }

  public String toJsonString() throws JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.writeValueAsString(this);
  }

  public List<RoundFile> getRoundFiles() {
    return roundFiles;
  }

  public void setRoundFiles(List<RoundFile> roundFiles) {
    this.roundFiles = roundFiles;
  }
}
