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
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.monitor.exception.EmptyStoreException;

/**
 * This is a singleton class.
 *
 * <p>Usage:<br> ConfigurationStore store =
 * ConfigurationStore.create().from(this.testXmlFile.getAbsolutePath()).build();<br> store.load();
 */

@XmlRootElement(name = "configurations")
@XmlType(propOrder = {"configurations"})
@XmlAccessorType(XmlAccessType.NONE)
public class ConfigurationStore {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationStore.class);
  private static final String referenceRemark = "ref:";
  private static final String SEPERATOR_CHECHER = 
      "[^\\w.*_#$%@!~&<>+\\_\\-=\\{\\}\\?\\-+\\^\\(\\)\\[\\]:]+";
  private static final String SUB_PROPERTY_SEPERATOR = ";";
  private static final int INDEX_OF_PROJECT = 0;
  private static final int INDEX_OF_FILE = 1;
  private static final int INDEX_OF_PROPERTY = 2;
  private static final int INDEX_OF_SUB_PROPERTY = 3;
  private String filePath;

  // XmlElement sets the name of the entities
  @XmlElement(name = "project")
  private List<ConfigProject> configurations;

  private ConfigurationStore() {
    configurations = new ArrayList<ConfigProject>();
  }

  private static ConfigurationStore getInstance() {
    return LazyHolder.singletonInstance;
  }

  public static Builder create() {
    return new Builder();
  }

  protected static String convertToPropertyFileFormat(List<ConfigSubProperty> subProperties) {
    String formattedProperty = "";
    for (ConfigSubProperty subProperty : subProperties) {
      formattedProperty += subProperty.getValue();
      formattedProperty += SUB_PROPERTY_SEPERATOR;
    }
    return formattedProperty
        .substring(0, formattedProperty.length() - SUB_PROPERTY_SEPERATOR.length());
  }

  public String getFilePath() {
    return filePath;
  }

  public void setFilePath(String filePath) {
    this.filePath = filePath;
  }

  public List<ConfigProject> getConfigurations() {
    return configurations;
  }

  public void setConfigurations(List<ConfigProject> configurations) {
    this.configurations = configurations;
  }

  /**
   * xx.
   */
  public void load() throws EmptyStoreException, Exception {
    logger.debug("Attribute repository xml file path : {}", filePath);

    File xmlFile = new File(filePath);
    if (!xmlFile.exists()) {
      xmlFile.createNewFile();
      throw new EmptyStoreException();
    } else {
      if (xmlFile.getTotalSpace() == 0L) {
        throw new EmptyStoreException();
      } else {
        FileReader fileReader = new FileReader(xmlFile);
        JAXBContext context = JAXBContext.newInstance(ConfigurationStore.class);
        Unmarshaller um = context.createUnmarshaller();
        ConfigurationStore configStore = (ConfigurationStore) um.unmarshal(fileReader);
        logger.debug("All configurations are {}", configStore);
        this.setConfigurations(configStore.getConfigurations());
      }
    }

    logger.debug("All configurations are: {}", configurations);
    try {
      dealWithRefference();
    } catch (Exception e) {
      logger.error("Failed to update configuration by refference", e);
    }
  }

  /**
   * xx.
   */
  public Map<String, Properties> getProperties(String projectName) {
    Map<String, Properties> fileName2Properties = new HashMap<String, Properties>();
    for (ConfigProject project : configurations) {
      List<ConfigFile> files = project.getFiles();
      if (!project.getName().equals("*") && project.getName().equals(projectName) == false) {
        continue;
      }
      if (files == null) {
        logger.debug("there is no propery files under {}", project.toString());
        continue;
      }

      for (ConfigFile file : files) {
        List<ConfigPropertyImpl> properties = file.getProperties();
        if (properties == null) {
          continue;
        }

        Properties configProperties = new Properties();
        for (ConfigPropertyImpl property : properties) {
          if (property.getValue() != null) {
            configProperties.put(property.getName(), property.getValue());
          } else {
            List<ConfigSubProperty> subProperties = property.getSubProperties();

            String value = convertToPropertyFileFormat(subProperties);
            configProperties.put(property.getName(), value);
          }
        }

        /*
         * Merge common properties and project 
         * specific properties. Project specific properties has higher
         * priority than common properties.
         */
        if (project.getName().equals("*")) {
          Properties projSpecs;

          projSpecs = fileName2Properties.get(file.getName());
          if (projSpecs != null) {
            configProperties.putAll(projSpecs);
          }
          fileName2Properties.put(file.getName(), configProperties);
        } else {
          Properties commonProps;

          commonProps = fileName2Properties.get(file.getName());
          if (commonProps != null) {
            commonProps.putAll(configProperties);
          } else {
            fileName2Properties.put(file.getName(), configProperties);
          }
        }
      }
    }

    return fileName2Properties;
  }

  /**
   * xx.
   */
  public void commit() throws Exception {
    logger.debug("Going to write data to disk, file path is : {}", filePath);

    // create JAXB context and instantiate marshaller
    JAXBContext context = JAXBContext.newInstance(ConfigurationStore.class);
    Marshaller mmarshaller = context.createMarshaller();
    mmarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

    // Write to File
    mmarshaller.marshal(this, new File(filePath));
  }

  /**
   * xx.
   */
  public List<String> listNameOfAllProjects() {
    List<String> projectNames = new ArrayList<String>();
    for (ConfigProject project : configurations) {
      projectNames.add(project.getName());
    }

    return projectNames;
  }

  public void add(ConfigProject project) {
    this.configurations.add(project);
  }

  private boolean isReference(String input) {
    int remarkLen = referenceRemark.length();
    if (input.length() < remarkLen) {
      return false;
    }
    String tmp = input.substring(0, remarkLen);
    return tmp.equals(referenceRemark);
  }

  private Vector<String> parseReference(String input) {
    Vector<String> results = new Vector<String>();
    StringTokenizer stringTokenizer = new StringTokenizer(input, "|");
    while (stringTokenizer.hasMoreElements()) {
      results.add((String) stringTokenizer.nextElement());
    }

    if (results.size() != 0) {
      String tmp = results.get(0);
      tmp = tmp.substring(referenceRemark.length(), tmp.length());
      results.set(0, tmp);
    }
    return results;
  }

  private void dealWithRefference() throws Exception {
    // for referrence function
    for (ConfigProject project : configurations) {
      List<ConfigFile> files = project.getFiles();
      if (files == null) {
        logger.debug("there is no file need to update under {}", project.toString());
        continue;
      }

      for (ConfigFile file : files) {
        List<ConfigPropertyImpl> properties = file.getProperties();
        if (properties == null) {
          continue;
        }

        for (ConfigPropertyImpl property : properties) {
          List<ConfigSubProperty> subProperties = property.getSubProperties();
          logger.debug("Current property is: {}", property);
          if (property.getValue() != null && subProperties == null) {
            logger.debug("NO SUB_PROPERTIES");
            // no sub-properties.
            updateValueAndRangeByRefference(property);
          } else {
            logger.debug("HAS SUB_PROPERTIES");
            for (ConfigSubProperty subProperty : subProperties) {
              updateValueAndRangeByRefference(subProperty);
            }
          }
        }
      }
    }
  }

  private void updateValueAndRangeByRefference(ConfigProperty obj) throws Exception {
    // value
    String value = obj.getValue();
    if (value != null && isReference(value)) {
      logger.debug("Current value is: {}", value);
      Vector<String> path = parseReference(value);
      try {
        PropertyValueAndRange newValue = getValueAndRangeOfProperty(path);
        obj.setValue(newValue.getValue());
      } catch (Exception e) {
        logger.warn("Caught an exception", e);
        // do not throw exception out, just continue
      }
    }

    // range
    String range = obj.getRange();
    if (range != null && isReference(range)) {
      Vector<String> path = parseReference(value);
      try {
        PropertyValueAndRange newValue = getValueAndRangeOfProperty(path);
        obj.setRange(newValue.getRange());
      } catch (Exception e) {
        logger.warn("Caught an exception", e);
        // do not throw exception out, just continue
      }
    }
  }

  /**
   * Get value&range from the specified path in configuration XML.
   */
  private PropertyValueAndRange getValueAndRangeOfProperty(Vector<String> path) throws Exception {
    logger.debug("Path is: {}", path);
    if (path.size() < 3) {
      logger.error("There is something wrong with the refference path");
      throw new Exception();
    }

    for (ConfigProject project : configurations) {
      if (project.getName().equals(path.get(INDEX_OF_PROJECT)) == false) {
        continue;
      }
      List<ConfigFile> files = project.getFiles();

      for (ConfigFile file : files) {
        logger.debug("Current file name is: {}, file name in path is: {}", file.getName(),
            path.get(INDEX_OF_FILE));
        if (file.getName().equals(path.get(INDEX_OF_FILE)) == false) {
          continue;
        }
        List<ConfigPropertyImpl> properties = file.getProperties();

        for (ConfigPropertyImpl property : properties) {
          logger.debug("Current property name is: {}, property name in path is: {}",
              property.getName(),
              path.get(INDEX_OF_PROPERTY));
          if (property.getName().equals(path.get(INDEX_OF_PROPERTY)) == false) {
            continue;
          }

          logger.debug("Current property: {}", property);
          if (property.getValue() != null) {
            return new PropertyValueAndRange(property.getValue(), property.getRange());
          } else {
            List<ConfigSubProperty> subProperties = property.getSubProperties();

            for (ConfigSubProperty subProperty : subProperties) {
              if (subProperty.getIndex().equals(path.get(INDEX_OF_SUB_PROPERTY)) == false) {
                continue;
              }
              return new PropertyValueAndRange(subProperty.getValue(), subProperty.getRange());
            }
          }
        }
      }
    }

    logger.error("There is no property : {}", path);
    throw new Exception();
  }

  @Override
  public String toString() {
    return "ConfigurationStore [filePath=" + filePath + ", configurations=" + configurations + "]";
  }

  private static class LazyHolder {

    private static final ConfigurationStore singletonInstance = new ConfigurationStore();
  }

  /**
   * the purpose of adding this class to make sure the file path had been set.
   */
  public static class Builder {

    private String xmlFilePath;

    public Builder from(String filePath) {
      xmlFilePath = filePath;
      return this;
    }

    /**
     * xx.
     */
    public ConfigurationStore build() {
      ConfigurationStore configrationStore = ConfigurationStore.getInstance();
      configrationStore.setFilePath(xmlFilePath);
      return configrationStore;
    }
  }

  private class PropertyValueAndRange {

    private String value;
    private String range;

    public PropertyValueAndRange(String value, String range) {
      this.value = value;
      this.range = range;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public String getRange() {
      return range;
    }

    public void setRange(String range) {
      this.range = range;
    }

    @Override
    public String toString() {
      return "PropertyValueAndRange [value=" + value + ", range=" + range + "]";
    }

  }

}
