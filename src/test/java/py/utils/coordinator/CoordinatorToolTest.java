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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.lang.Validate;
import org.junit.Test;
import py.test.TestBase;

/**
 * xx.
 */
public class CoordinatorToolTest extends TestBase {

  public CoordinatorToolTest() throws Exception {
    super.init();
  }

  @Test
  public void patternMatch() throws Exception {
    Pattern pattern = Pattern.compile("_");
    String[] results = pattern.split("/fsdf/sdf_1sf/fasdfa_1");
    for (String str : results) {
      logger.info("result={}", str);
    }

    Validate.isTrue(Integer.valueOf(results[results.length - 1]) == 1);
  }

  @Test
  public void dataWrapperSerialize() throws Exception {

  }

  @Test
  public void roundFileManager() throws Exception {
    RoundFileManager manager1 = new RoundFileManager();
    manager1.setVolumeId(100L);
    manager1.setAccountId(1000L);
    manager1.setDomainId(10000L);
    manager1.setStoragePoolId(100000L);
    manager1.setVolumeName("fff");
    manager1.create(true);
    List<RoundFile> roundFiles = new ArrayList<RoundFile>();
    int count = 10;
    for (int i = 0; i < count; i++) {
      RoundFile roundFile = manager1.generateNewRoundFile(i);
      roundFiles.add(roundFile);
    }
    manager1.setRoundFiles(roundFiles);

    manager1.flush();

    RoundFileManager manager2 = RoundFileManager.load();

    assertTrue(manager1.getVolumeId() == manager2.getVolumeId());
    assertTrue(manager1.getAccountId() == manager2.getAccountId());
    assertTrue(manager1.getStoragePoolId() == manager2.getStoragePoolId());
    assertTrue(manager1.getDomainId() == manager2.getDomainId());
    assertTrue(manager1.getVolumeName().compareTo(manager2.getVolumeName()) == 0);
    assertTrue(manager1.getRoundFiles().size() == count);
    assertTrue(manager2.getRoundFiles().size() == count);

  }

  @Test
  public void roundFile() throws Exception {

  }

}
