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

package py.service.configuration.manager;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import org.junit.Test;

/**
 * xx.
 */
public class ConfigurationManagerTest {

  @Test
  public void updateYamlFileTest()
      throws NoSuchMethodException, IOException, InvocationTargetException, IllegalAccessException {

    ConfigurationManager configurationManager = new ConfigurationManager();
    Method method = configurationManager.getClass()
        .getDeclaredMethod("updateYamlFile", String.class, Properties.class);
    method.setAccessible(true);
    String yamlStr = "/home/wjf/Downloads/resttest/application.yml";
    String proStr = "/home/wjf/Downloads/resttest/app_replace.properties";

    Properties properties = new Properties();
    properties.load(new FileInputStream(proStr));

    method.invoke(configurationManager, yamlStr, properties);
  }

}