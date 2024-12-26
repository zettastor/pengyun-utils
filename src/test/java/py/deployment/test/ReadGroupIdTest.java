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

package py.deployment.test;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;
import py.instance.Group;
import py.test.TestBase;

/**
 * xx.
 */
public class ReadGroupIdTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ReadGroupIdTest.class);

  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    String directory = "config";
    String filename = "deploy.properties";
    File file = new File(directory, filename);
    if (file.exists()) {
      file.delete();
    } else {
      file.getParentFile().mkdirs();
      try {
        file.createNewFile();
      } catch (IOException e) {
        logger.error("Can not find the path");
        throw new IllegalStateException(e);

      }
    }

    FileWriter fileWriter;
    FileReader fileReader;
    String str1 = "DataNode.deployment.host.group.enabled=true";
    String str2 = "DataNode.deployment.host.group.0=10.0.1.101:10.0.1.103";
    String str3 = "DataNode.deployment.host.group.1=10.0.1.104:10.0.1.106";
    String str4 = "DataNode.deployment.host.group.2=10.0.1.107:10.0.1.109";
    try {
      fileWriter = new FileWriter("config/deploy.properties");
      BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
      bufferedWriter.write(str1);
      bufferedWriter.newLine();
      bufferedWriter.write(str2);
      bufferedWriter.newLine();
      bufferedWriter.write(str3);
      bufferedWriter.newLine();
      bufferedWriter.write(str4);
      bufferedWriter.newLine();
      bufferedWriter.flush();
      bufferedWriter.close();
    } catch (IOException e) {
      logger.error("No file or directory");
      throw new IllegalStateException(e);
    }
  }

  @Test
  public void test() throws Exception {
    Group group = new Group();
    group.setGroupId(0);
    List<String> group0 = new ArrayList();
    group0.add("10.0.1.101");
    group0.add("10.0.1.102");
    group0.add("10.0.1.103");
    List<String> ipList = new DataNodeDeploymentConfiguration().getDeploymentHosts(group);
    assertEquals(group0, ipList);


  }

  /**
   * xx.
   */
  @After
  public void clean() throws Exception {
    File file = new File("config/deploy.properties");
    if (file.exists()) {
      file.delete();
    }
  }


}
