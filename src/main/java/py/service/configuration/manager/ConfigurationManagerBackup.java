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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;

/**
 * A command line utils to change properties under services.
 */
public class ConfigurationManagerBackup {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationManagerBackup.class);

  /**
   * xx.
   */
  public static void main(String[] args) {
    Path allServicePackagePath = Paths.get(args[0]);
    Path xmlConfigurationFile = Paths.get(args[1]);

    XmlConfigurationFileReader xmlReader = new XmlConfigurationFileReader(xmlConfigurationFile);
    logger.info("Going to Read the xml file: {}", xmlConfigurationFile.toString());
    List<String> allProjects = xmlReader.listNameOfAllProjects();
    logger.info("All Projects are as follows: {}", allProjects.toString());

    String[] servicePackageNames = allServicePackagePath.toFile().list();

    String serviceNames = "";
    for (String servicePackagename : servicePackageNames) {
      serviceNames += servicePackagename;
    }
    logger.info("Service-packages are: {}", serviceNames);

    for (String servicePackageName : servicePackageNames) {
      logger.info("*** Try to check Packages: {}", servicePackageName);
      String projectToApplyNewConfiguration = null;
      for (String projectName : allProjects) {
        if (servicePackageName.contains(projectName)) {
          projectToApplyNewConfiguration = projectName;
          break;
        }
      }

      if (projectToApplyNewConfiguration == null) {
        logger.info("Service {} will not apply new configuration", servicePackageName);
        continue;
      }

      Path servicePackagePath = Paths.get(allServicePackagePath.toString(), servicePackageName);
      if (!servicePackagePath.toFile().isDirectory()) {
        logger.info("Service {} will not apply new configuration because {} is not a directory",
            servicePackageName, servicePackagePath);
        continue;
      }

      logger.info("*** Applying properties to \"{}\"", projectToApplyNewConfiguration);

      Map<String, Properties> fileName2Properties = xmlReader
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
        FileInputStream finput = null;
        try {
          finput = new FileInputStream(configFilePath.toFile());
        } catch (FileNotFoundException e) {
          logger.warn("Unable to find config file {}", configFilePath);
          continue;
        }

        Properties properties = new Properties();
        try {
          properties.load(finput);
          finput.close();
        } catch (IOException e) {
          logger.error("Caught an exception", e);
          System.exit(1);
        }

        logger.info("File: {}\n\tProperties: {}", propertiesFileName,
            fileName2Properties.get(propertiesFileName));
        Properties newProperties = fileName2Properties.get(propertiesFileName);
        for (Object key : newProperties.keySet()) {
          properties.put(key, newProperties.get(key));
        }

        FileOutputStream foutput = null;
        try {
          foutput = new FileOutputStream(configFilePath.toFile());
          properties.store(foutput, null);
          foutput.close();
        } catch (FileNotFoundException e) {
          logger.error("Unable to find config file {}", configFilePath, e);
          System.exit(1);
        } catch (IOException e) {
          logger.error("Caught an exception", e);
          System.exit(1);
        }
      }
    }

    System.exit(0);
  }
}
