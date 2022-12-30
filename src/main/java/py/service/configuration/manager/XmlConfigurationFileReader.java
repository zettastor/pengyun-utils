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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.Vector;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * 1.add reference idea into the XML-configuration file . by sxl The reference schema is :
 * "ref":"project name"|"file name"|"property name". "ref": is a mark that indicate the value is a
 * reference value.
 *
 * <p>"project name" is the tag in xml such as
 * <xml><project name="pengyun-console"/>"</xml>
 *
 * <p>"file name" is the tag in xml such as
 * <xml><file name="console.properties"/></xml>
 *
 * <p>"property name" is the tag in xml such as
 * <xml><property name="zookeeper.election.switch" value="true" range="true;false"/></xml>
 *
 * <p>eg:<xml><property name="jmx.agent.port.monitorcenter"
 * value="ref:pengyun-system_monitor|jmxagent.properties|jmx.agent.port" /></xml>
 */
public class XmlConfigurationFileReader {

  private static final Logger logger = LoggerFactory.getLogger(XmlConfigurationFileReader.class);
  private static final String referenceRemark = "ref:";

  private Document xmlDoc;

  /**
   * xx.
   */
  public XmlConfigurationFileReader(Path xmlConfigurationFilePath) {
    try {
      DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      xmlDoc = builder.parse(xmlConfigurationFilePath.toFile());
      xmlDoc.getDocumentElement().normalize();

      // make into the real value instead of reference value
      parseReference();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw new RuntimeException("Unable to init class " + getClass().getName());
    }
  }

  /**
   * xx.
   */
  public List<String> listNameOfAllProjects() {
    List<String> projectNameList = new ArrayList<String>();
    NodeList projectNodeList = xmlDoc.getElementsByTagName("project");

    for (int i = 0; i < projectNodeList.getLength(); i++) {
      Node projectNode = projectNodeList.item(i);
      if (projectNode.getNodeType() == Node.ELEMENT_NODE) {
        Element projectElement = (Element) projectNode;
        projectNameList.add(projectElement.getAttribute("name"));
      }
    }

    logger.info("Project list is: {}", projectNameList);
    return projectNameList;
  }

  /**
   * xx.
   */
  public Map<String, Properties> getProperties(String projectName) {
    Map<String, Properties> fileName2Properties = new HashMap<String, Properties>();

    NodeList projectNodeList = xmlDoc.getElementsByTagName("project");
    for (int i = 0; i < projectNodeList.getLength(); i++) {
      Node projectNode = projectNodeList.item(i);
      if (projectNode.getNodeType() == Node.ELEMENT_NODE) {
        Element projectElement = (Element) projectNode;
        if (projectElement.getAttribute("name").equals("*")) {
          for (Entry<String, Properties> entry : getProperties(projectElement).entrySet()) {
            fileName2Properties.put(entry.getKey(), entry.getValue());
            logger
                .debug("Common Property : Name - {}, Value - {}", entry.getKey(), entry.getValue());
          }
          break;
        }
      }
    }

    for (int i = 0; i < projectNodeList.getLength(); i++) {
      Node projectNode = projectNodeList.item(i);
      if (projectNode.getNodeType() == Node.ELEMENT_NODE) {
        Element projectElement = (Element) projectNode;
        if (projectElement.getAttribute("name").contains(projectName)) {
          for (Entry<String, Properties> entry : getProperties(projectElement).entrySet()) {
            fileName2Properties.put(entry.getKey(), entry.getValue());
            logger.debug("Special Property : Name - {}, Value - {}", entry.getKey(),
                entry.getValue());
          }
          break;
        }
      }
    }

    return fileName2Properties;
  }

  /**
   * xx.
   */
  public Map<String, Properties> getProperties(Element projectElement) {
    Map<String, Properties> fileName2Properties = new HashMap<String, Properties>();

    NodeList fileNodeList = projectElement.getElementsByTagName("file");
    for (int i = 0; i < fileNodeList.getLength(); i++) {
      Node fileNode = fileNodeList.item(i);
      if (fileNode.getNodeType() == Node.ELEMENT_NODE) {
        Element fileElement = (Element) fileNode;
        Properties properties = new Properties();
        fileName2Properties.put(fileElement.getAttribute("name"), properties);

        NodeList propertyNodeList = fileElement.getElementsByTagName("property");
        for (int j = 0; j < propertyNodeList.getLength(); j++) {
          Node propertyNode = propertyNodeList.item(j);
          if (propertyNode.getNodeType() == Node.ELEMENT_NODE) {
            Element propertyElement = (Element) propertyNode;
            String name = propertyElement.getAttribute("name");
            String value = propertyElement.getAttribute("value");
            String range = propertyElement.getAttribute("range");
            logger.debug("name:{}, value:{}, range:{}", name, value, range);

            if (!isInRange(value, range)) {
              logger.error("value {} of property {} is not in range {} for project {}", value, name,
                  range, projectElement.getAttribute("name"));
              return null;
            }

            properties.put(name, value);
          }
        }
      }
    }

    return fileName2Properties;
  }

  /**
   * get property value of the specified key.
   */
  public String getProperty(String projectName, String fileName, String propertyKey) {
    Map<String, Properties> projectProperties = getProperties(projectName);
    Properties properties = projectProperties.get(fileName);
    logger.debug("project name: {}, file name: {}, property key: {}. properties : {}", projectName,
        fileName,
        propertyKey, properties);
    return properties.getProperty(propertyKey);
  }

  /**
   * xx.
   */
  public String getProperty(Vector<String> path) {
    if (path.size() != 3) {
      return "";
    }
    return getProperty(path.get(0), path.get(1), path.get(2));
  }

  /**
   * xx.
   */
  public boolean isInRange(String value, String range) {
    if (range == null || range.isEmpty()) {
      return true;
    }

    for (String itemInRange : range.split(";")) {
      if (itemInRange.equals(value)) {
        return true;
      }
    }

    return false;
  }

  protected boolean isReference(String input) {
    int remarkLen = referenceRemark.length();
    if (input.length() < remarkLen) {
      return false;
    }
    String tmp = input.substring(0, remarkLen);
    return tmp.equals(referenceRemark);
  }

  protected Vector<String> parseReference(String input) {
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

  private void parseReference() throws Exception {
    NodeList projectNodeList = xmlDoc.getElementsByTagName("project");
    for (int i = 0; i < projectNodeList.getLength(); i++) {
      Element projectNode = (Element) projectNodeList.item(i);
      NodeList fileNodeList = projectNode.getElementsByTagName("file");

      for (int j = 0; j < fileNodeList.getLength(); j++) {
        Element fileElement = (Element) fileNodeList.item(j);
        NodeList propertyNodeList = fileElement.getElementsByTagName("property");

        for (int k = 0; k < propertyNodeList.getLength(); k++) {
          Node propertyNode = propertyNodeList.item(k);
          Element propertyElement = (Element) propertyNode;
          String value = propertyElement.getAttribute("value");
          if (isReference(value)) {
            Vector<String> path = parseReference(value);
            String newValue = getProperty(path);
            propertyElement.setAttribute("value", newValue);
          }

          String range = propertyElement.getAttribute("range");
          if (isReference(range)) {
            Vector<String> path = parseReference(range);
            String newRange = getProperty(path);
            propertyElement.setAttribute("range", newRange);
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return "XMLConfigurationFileReader [xmlDoc=" + xmlDoc + "]";
  }

}
