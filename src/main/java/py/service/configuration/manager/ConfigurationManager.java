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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import py.common.PyService;

/**
 * xx.
 */
public class ConfigurationManager {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationManager.class);

  /**
   * xx.
   */
  public static void main(String[] args) {
    String logOut = "";
    Path allServicePackagePath = Paths.get(args[0]);
    String xmlConfigurationFile = args[1];

    ConfigurationStore configurationStore = ConfigurationStore.create().from(xmlConfigurationFile)
        .build();
    try {
      configurationStore.load();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      return;
    }

    // logger.info("Going to Read the xml File: {}", xmlConfigurationFile.toString());
    List<String> allProjects = configurationStore.listNameOfAllProjects();
    // logger.info("All Projects are as follows: {}", allProjects.toString());

    String[] servicePackageNames = allServicePackagePath.toFile().list();

    for (String servicePackageName : servicePackageNames) {
      String projectToApplyNewConfiguration = null;
      for (String projectName : allProjects) {
        if (servicePackageName.contains(projectName)) {
          projectToApplyNewConfiguration = projectName;
          break;
        }
      }

      if (projectToApplyNewConfiguration == null) {
        // logger.info("Service {} will not apply new configuration", servicePackageName);
        continue;
      }

      Path servicePackagePath = Paths.get(allServicePackagePath.toString(), servicePackageName);
      if (!servicePackagePath.toFile().isDirectory()) {
        // logger.info("Service {} will not apply new configuration because {} is not a directory",
        // servicePackageName, servicePackagePath);
        continue;
      }

      // logger.info("*** Applying properties to \"{}\"", projectToApplyNewConfiguration);
      logOut += "\n+--------------------------------------" 
          + "-----------------------------------------+\n";
      logOut += String.format("                           %s", projectToApplyNewConfiguration);
      logOut += "\n+--------------------------------" 
          + "-----------------------------------------------+\n";

      Map<String, Properties> fileName2Properties = configurationStore
          .getProperties(projectToApplyNewConfiguration);
      for (String propertiesFileName : fileName2Properties.keySet()) {
        Path configFilePath = Paths
            .get(allServicePackagePath.toString(), servicePackageName, "config",
                propertiesFileName);
        if (projectToApplyNewConfiguration.contains(PyService.CONSOLE.getServiceProjectKeyName())
            && propertiesFileName.contains("log4j.properties")) {
          configFilePath = Paths.get(allServicePackagePath.toString(), servicePackageName,
              "/tomcat/webapps/ROOT/WEB-INF/classes", propertiesFileName);
        } else if (
            projectToApplyNewConfiguration.contains(PyService.CONSOLE.getServiceProjectKeyName())
                && propertiesFileName.contains("metric.properties")) {
          logger.warn("property file metric.properties is not used to {}",
              projectToApplyNewConfiguration);
          continue;
        } else if (projectToApplyNewConfiguration
            .contains(PyService.CONSOLE.getServiceProjectKeyName())) {
          configFilePath = Paths.get(allServicePackagePath.toString(), servicePackageName,
              "/tomcat/webapps/ROOT/WEB-INF/classes/config", propertiesFileName);
        }

        try {
          if (propertiesFileName.endsWith("properties")) {
            updatePropertiesFile(configFilePath.toFile(),
                fileName2Properties.get(propertiesFileName));
          } else if (propertiesFileName.endsWith("yml") || propertiesFileName.endsWith("yaml")) {

            updateYamlFile(configFilePath.toFile().getPath(),
                fileName2Properties.get(propertiesFileName));
          }
        } catch (IOException e) {
          continue;
        }
      }
    }

    logger.info("\n{}", logOut);
    System.exit(0);
  }


  private static void updatePropertiesFile(File configFilePath, Properties newProperties)
      throws FileNotFoundException {
    FileInputStream finput = null;
    try {
      finput = new FileInputStream(configFilePath);
    } catch (FileNotFoundException e) {
      logger.warn("Unable to find config file {}", configFilePath.getAbsolutePath());
      throw e;
    }

    Properties properties = new Properties();
    try {
      properties.load(finput);
      finput.close();
    } catch (IOException e) {
      logger.error("Caught an exception", e);
      System.exit(1);
    }

    String logOut = "";
    logOut += String.format("\tFile: %s\n\tProperties: %s", configFilePath.getName(),
        formatProperties(newProperties.toString()));
    for (Object key : newProperties.keySet()) {
      properties.put(key, newProperties.get(key));
    }

    FileOutputStream foutput = null;
    try {
      foutput = new FileOutputStream(configFilePath);
      properties.store(foutput, null);
      foutput.getFD().sync();
      foutput.flush();
    } catch (FileNotFoundException e) {
      logger.error("Unable to find config file {}", configFilePath.getAbsolutePath(), e);
      System.exit(1);
    } catch (IOException e) {
      logger.error("Caught an exception", e);
      System.exit(1);
    } finally {
      if (foutput != null) {
        try {
          foutput.close();
        } catch (IOException e) {
          logger.error("Unable to close file {}", configFilePath.getAbsolutePath(), e);
        }
      }
    }
  }

  private static void updateYamlFile(String yamlPath, Properties properties)
      throws IOException {
    Map<String, Object> assignmentMap = new HashMap<>();
    Map<String, Object> yamlToMap = new HashMap<>();
    Yaml yaml = new Yaml();
    //get yaml file context，to map
    yamlToMap = yaml.load(new FileInputStream(yamlPath));
    assignmentMap = recursionMapAssignment(yamlToMap, properties, null);
    yaml.dump(assignmentMap, new FileWriter(yamlPath));
  }

  private static Map<String, Object> recursionMapAssignment(Map<String, Object> yamlToMap,
      Properties propertiesMap, String lastKey) {
    Map<String, Object> yamlToMapRet = new LinkedHashMap<>();

    for (String yamlKey : yamlToMap.keySet()) {
      Object yamlValue = yamlToMap.get(yamlKey);

      String currentKey = yamlKey;
      if (!StringUtils.isEmpty(lastKey)) {
        currentKey = String.format("%s.%s", lastKey, yamlKey);
      }

      // if yamlValue is map instance
      if (yamlValue instanceof Map) {
        yamlToMapRet.put(yamlKey, recursionMapAssignment((Map<String, Object>) yamlValue,
            propertiesMap, currentKey));
        continue;
      }

      // if yamlValue is a single type
      Object newValue = yamlValue;
      try {
        newValue = propertiesMap.get(currentKey);
        if (newValue == null) {
          logger.debug("not found key:{} in properties file", currentKey);
          newValue = yamlValue;
        }
      } catch (Exception e) {
        logger.debug("not found key:{} in properties file", currentKey);
      }

      if (yamlValue instanceof Integer) {
        yamlToMapRet.put(yamlKey, Integer.valueOf(String.valueOf(newValue)));
      } else {
        yamlToMapRet.put(yamlKey, newValue);
      }
    }

    return yamlToMapRet;
  }

  private static String formatProperties(String properties) {
    properties = properties.replaceAll("\\s", "");
    String formattedPorperties = properties;
    formattedPorperties = formattedPorperties.replaceAll("[,\\{\\}]", "\n\t\t");
    formattedPorperties += "\n";

    return formattedPorperties;
  }
}
