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