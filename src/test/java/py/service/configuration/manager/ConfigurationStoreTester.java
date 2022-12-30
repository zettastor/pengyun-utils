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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.monitor.exception.EmptyStoreException;
import py.test.TestBase;

/**
 * xx.
 */
public class ConfigurationStoreTester extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(ConfigurationStoreTester.class);
  protected File testXmlFile;

  @Before
  public void init() throws Exception {
    super.init();
  }

  private void loadDataFromXml() throws Exception {
    testXmlFile = new File("/tmp/test.xml");
    if (testXmlFile.exists()) {
      testXmlFile.delete();
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(testXmlFile));
    // bw.write(xmlBuilder.toString());
    bw.write(getTestingXml());
    bw.flush();
    bw.close();
  }

  private void loadDataFromXml1() throws Exception {
    testXmlFile = new File("/tmp/test.xml");
    if (testXmlFile.exists()) {
      testXmlFile.delete();
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(testXmlFile));
    // bw.write(xmlBuilder.toString());
    bw.write(getTestingXml1());
    bw.flush();
    bw.close();
  }

  @Test
  public void testLoad() throws EmptyStoreException, Exception {
    try {
      loadDataFromXml();
      ConfigurationStore store = ConfigurationStore.create()
          .from(this.testXmlFile.getAbsolutePath()).build();
      store.load();
      logger.debug("{}", store);
      store.commit();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }

  @Test
  public void testLoad1() throws EmptyStoreException, Exception {
    try {
      loadDataFromXml1();
      ConfigurationStore store = ConfigurationStore.create()
          .from(this.testXmlFile.getAbsolutePath()).build();
      store.load();
      logger.debug("{}", store);
      store.commit();
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }

  @Test
  public void testConvertToPropertyFileFormat() {
    ConfigSubProperty subProperty1 = new ConfigSubProperty();
    subProperty1.setIndex("1");
    subProperty1.setValue("a");
    subProperty1.setRange("");

    ConfigSubProperty subProperty2 = new ConfigSubProperty();
    subProperty2.setIndex("1");
    subProperty2.setValue("b");
    subProperty2.setRange("");

    ConfigSubProperty subProperty3 = new ConfigSubProperty();
    subProperty3.setIndex("1");
    subProperty3.setValue("c");
    subProperty3.setRange("");
    List<ConfigSubProperty> testSubProperties = new ArrayList<ConfigSubProperty>();
    testSubProperties.add(subProperty1);
    testSubProperties.add(subProperty2);
    testSubProperties.add(subProperty3);
    String formattedProperty = ConfigurationStore.convertToPropertyFileFormat(testSubProperties);
    logger.debug("{}", formattedProperty);
    Assert.assertEquals("a;b;c", formattedProperty);
  }

  @Test
  public void testCommit() throws EmptyStoreException, Exception {
    loadDataFromXml();
    try {
      final ConfigurationStore store = ConfigurationStore.create()
          .from(this.testXmlFile.getAbsolutePath()).build();
      List<ConfigSubProperty> values = new ArrayList<ConfigSubProperty>();
      ConfigSubProperty value = new ConfigSubProperty();
      value.setValue("v1");
      value.setRange("");
      values.add(value);

      List<ConfigPropertyImpl> properties = new ArrayList<ConfigPropertyImpl>();
      ConfigPropertyImpl property = new ConfigPropertyImpl();
      property.setName("testing property name");
      property.setSubProperties(values);
      properties.add(property);

      List<py.service.configuration.manager.ConfigFile> files = 
          new ArrayList<py.service.configuration.manager.ConfigFile>();
      py.service.configuration.manager.ConfigFile file = 
          new py.service.configuration.manager.ConfigFile();
      file.setName("testing file name");
      file.setProperties(properties);
      files.add(file);

      List<ConfigProject> configurations = new ArrayList<ConfigProject>();
      ConfigProject project = new ConfigProject();
      project.setName("test");
      project.setFiles(files);
      configurations.add(project);

      store.add(project);

      store.commit();
      logger.debug("{}", store);
    } catch (Exception e) {
      logger.error("Caught an exception", e);
      throw e;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetProperties() {
    final String metricPropsFileName = "metric.properties";
    final String keyMetricEnable = "metric.enable";
    final String keyMetricEnablePrefix = "metric.enable.prefix";
    final String keyMetricPort = "metric.port";

    ConfigFile metricConfig;
    List<ConfigProject> projConfigs;

    projConfigs = new ArrayList<ConfigProject>();

    /*
     * Common configuration
     */
    metricConfig = new ConfigFile();
    metricConfig.setName(metricPropsFileName);
    metricConfig.setProperties(
        Arrays.asList(new ConfigPropertyImpl[]{new ConfigPropertyImpl(keyMetricEnable, "true"),
            new ConfigPropertyImpl(keyMetricEnablePrefix, "common.metric")}));
    ConfigProject commonProjConfig;
    commonProjConfig = new ConfigProject();
    commonProjConfig.setName("*");
    commonProjConfig.setFiles(Arrays.asList(new ConfigFile[]{metricConfig}));

    projConfigs.add(commonProjConfig);

    /*
     * DIH configuration
     */
    metricConfig = new ConfigFile();
    metricConfig.setName(metricPropsFileName);
    metricConfig.setProperties(Arrays.asList(new ConfigPropertyImpl[]{
        new ConfigPropertyImpl(keyMetricEnablePrefix, "dih.metric"),
        new ConfigPropertyImpl(keyMetricPort, "10100")}));
    ConfigProject dihConfig;
    dihConfig = new ConfigProject();
    dihConfig.setName("dih");
    dihConfig.setFiles(Arrays.asList(new ConfigFile[]{metricConfig}));

    projConfigs.add(dihConfig);

    Properties properties;
    ConfigurationStore configurationStore;

    configurationStore = new ConfigurationStore.Builder().build();
    configurationStore.setConfigurations(projConfigs);
    properties = configurationStore.getProperties("dih").get(metricPropsFileName);

    org.junit.Assert.assertEquals(3, properties.size());
    org.junit.Assert.assertEquals("true", properties.get(keyMetricEnable));
    org.junit.Assert.assertEquals("dih.metric", properties.get(keyMetricEnablePrefix));
    org.junit.Assert.assertEquals("10100", properties.get(keyMetricPort));
  }

  // ref:pengyun-infocenter|metric.properties|metric.prefix
  protected String getTestingXml() {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<configurations>\n"
        + "    <project name=\"pengyun-infocenter\">\n"
        + "        <file name=\"test.properties\">\n"
        + "        </file>\n" + "        <file name=\"jmxagent.properties\">\n"
        + "                        <property name=\"jmx.agent.port\" value=\"11111\"/>\n"
        + "                        <property name=\"jmx.agent.test\">\n"
        + "                                <sub_property index=\"0\" value=\"0000\" />\n"
        + "                        </property>\n" + "        </file>\n" + "    </project>\n"
        + "</configurations>\n";
  }

  private String getTestingXml1() {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "<configurations>"
        + "<project name=\"*\">"
        + "<file name=\"storage.properties\">"
        + "<property name=\"page.size.byte\" value=\"65536\" />"
        + "<!-- 1G -->" + "<property name=\"segment.size.byte\" value=\"1073741824\" />" + "</file>"
        + "<file name=\"network.properties\">"
        + "<property name=\"control.flow.subnet\" value=\"10.0.1.0/24\" />" + "</file>"

        + "<file name=\"log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"WARN, FILE\"/>"
        + "</file>" + "<file name=\"metric.properties\">"
        + "<property name=\"metric.enable.profiles\" value=\"metric.jmx\" " 
        + "range=\"metric.jmx;metric.graphite;metric.csv;metric.console;metri.slf4j\" />"
        + "</file>" + "</project>" + "\n" + "<project name=\"pengyun-console\">"
        + "<file name=\"console.properties\">" + "</file>" + "</project>" + "\n"
        + "<project name=\"pengyun-controlcenter\">" + "<file name=\"controlcenter.properties\">"
        + "<property name=\"zookeeper.election.switch\" value=\"true\" range=\"true;false\"/>"
        + "<property name=\"jdbc.driver.class\" value=\"org.postgresql.Driver\" />"
        + "<property name=\"jdbc.url\" " 
        + "value=\"jdbc:postgresql://10.0.1.128:5432/controlcenterdb\" />"
        + "<property name=\"jdbc.initial.pool.size\" value=\"5\" />"
        + "<property name=\"jdbc.min.pool.size\" value=\"5\" />"
        + "<property name=\"jdbc.max.pool.size\" value=\"30\" />"
        + "<!-- use zookeeper store in case of integretion test start-->"
        + "<property name=\"license.storage.type\" value=\"zookeeper\" range=\"file;zookeeper\"/>"
        + "</file>"
        + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.integration_test.controlcenter\" />"
        + "</file>"
        + "<file name=\"log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"WARN,  FILE\"/>"
        + "</file>" + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"8110\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "\n" + "<project name=\"pengyun-coordinator\">"
        + "<file name=\"coordinator-log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"ERROR, FILE\"/>" + "</file>"
        + "<file name=\"coordinator.properties\">" + "</file>"
        + "<file name=\"coordinator-jvm.properties\">"
        + "<property name=\"initial.mem.pool.size\" value=\"1024m\"/>"
        + "<property name=\"min.mem.pool.size\" value=\"1024m\"/>"
        + "<property name=\"max.mem.pool.size\" value=\"1024m\"/>" + "</file>"
        + "<file name=\"drivercontainer.properties\">" + "</file>"
        + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.integration_test.coordinator\" />"
        + "</file>"
        + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"9100\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "\n" + "<project name=\"pengyun-datanode\">" + "<file name=\"datanode.properties\">"
        + "<property name=\"janitor.execution.rate.ms\" value=\"60000\" />"
        + "<property name=\"request.new.segment.unit.expiration.threshold.ms\" value=\"80000\"/>"
        + "<property name=\"threshold.to.remove.inactive.secondary.ms\" value=\"120000\" />"
        + "<property name=\"wait.time.ms.to.move.segment.to.deleted\" value=\"60000\" />"
        + "<property name=\"memory.size.for.data.logs.mb\" value=\"100\" />"
        + "<property name=\"file.buffer.size.gb\" value=\"1\" />"
        + "<property name=\"enable.ssd.for.data.log\" value=\"false\" />"
        + "<property name=\"enable.file.buffer\" value=\"false\" />" + "</file>"
        + "<file name=\"coordinator-log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"ERROR, FILE\"/>" + "</file>"
        + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.integration_test.datanode\" />"
        + "</file>"
        + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"10111\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "\n" + "<project name=\"pengyun-deployment_daemon\">"
        + "<file name=\"deployment_daemon.properties\">"
        + "</file>" + "</project>" + "\n" + "<project name=\"pengyun-infocenter\">"
        + "<file name=\"infocenter.properties\">"
        + "<property name=\"zookeeper.election.switch\" value=\"true\" range=\"true;false\"/>"
        + "<property name=\"dead.volume.to.remove.second\" value=\"300\" />"
        + "<property name=\"jdbc.driver.class\" value=\"org.postgresql.Driver\" />"
        + "<property name=\"jdbc.url\" value=\"jdbc:postgresql://10.0.1.128:5432/infocenterdb\" />"
        + "<property name=\"jdbc.initial.pool.size\" value=\"5\" />"
        + "<property name=\"jdbc.min.pool.size\" value=\"5\" />"
        + "<property name=\"instance.metadata.to.remove\" value=\"30000\" />"
        + "<property name=\"jdbc.max.pool.size\" value=\"30\" />" + "</file>"
        + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.integration_test.infocenter\" />"
        + "</file>"
        + "<file name=\"log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"WARN, FILE\"/>"
        + "</file>" + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"8120\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "\n" + "<project name=\"pengyun-instancehub\">" + "<file name=\"instancehub.properties\">"
        + "<property name=\"center.dih.endpoint\" value=\"10.0.1.128:10000\" />"
        + "<property name=\"time.ok.to.inc\" value=\"5000\" />"
        + "<property name=\"time.failed.to.forgotten.of.local\" value=\"10000\" />"
        + "<property name=\"time.failed.to.forgotten\" value=\"10000\" />"
        + "<property name=\"time.forgotten.to.remove.of.local\" value=\"10000\" />"
        + "<property name=\"time.forgotten.to.remove\" value=\"10000\" />" + "\n" + "</file>"
        + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.integration_test.instancehub\" />"
        + "</file>"
        + "<file name=\"log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"WARN, FILE\"/>"
        + "</file>" + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"10100\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "\n" + "<project name=\"pengyun-system_monitor\">"
        + "<file name=\"monitorcenter.properties\">"
        + "<property name=\"zookeeper.election.switch\" value=\"true\" range=\"true;false\"/>"
        + "<property name=\"jdbc.driver.class\" value=\"org.postgresql.Driver\" />"
        + "<property name=\"jdbc.url\" " 
        + "value=\"jdbc:postgresql://10.0.1.128:5432/monitorcenterdb\" />"
        + "<property name=\"jdbc.initial.pool.size\" value=\"5\" />"
        + "<property name=\"jdbc.min.pool.size\" value=\"5\" />"
        + "<property name=\"jdbc.max.pool.size\" value=\"30\" />"
        + "<property name=\"attribute.update.rate\" value=\"86400000\" />"
        + "<property name=\"attribute.update.delay\" value=\"30000\" />" + "\n"
        + "<property " 
        + "name=\"jmx.agent.port.inforcenter\" " 
        + "value=\"ref:pengyun-infocenter|jmxagent.properties|jmx.agent.port\" />"
        + "<property name=\"jmx.agent.port.controlcenter\" " 
        + "value=\"ref:pengyun-controlcenter|jmxagent.properties|jmx.agent.port\" />"
        + "<property name=\"jmx.agent.port.drivercontainer\" " 
        + "value=\"ref:pengyun-coordinator|jmxagent.properties|jmx.agent.port\" />"
        + "<property name=\"jmx.agent.port.datanode\" " 
        + "value=\"ref:pengyun-datanode|jmxagent.properties|jmx.agent.port\" />"
        + "<property name=\"jmx.agent.port.dih\" " 
        + "value=\"ref:pengyun-instancehub|jmxagent.properties|jmx.agent.port\" />"
        + "<property name=\"jmx.agent.port.monitorcenter\" " 
        + "value=\"ref:pengyun-system_monitor|jmxagent.properties|jmx.agent.port\" />"
        + "\n"
        + "<property name=\"metric.domain.infocenter\" " 
        + "value=\"ref:pengyun-infocenter|metric.properties|metric.prefix\"/>"
        + "<property name=\"metric.domain.controcenter\" " 
        + "value=\"ref:pengyun-controlcenter|metric.properties|metric.prefix\"/>"
        + "<property name=\"metric.domain.drivercontainer\"" 
        + " value=\"ref:pengyun-coordinator|metric.properties|metric.prefix\"/>"
        + "<property name=\"metric.domain.datanode\" " 
        + "value=\"ref:pengyun-datanode|metric.properties|metric.prefix\"/>"
        + "<property name=\"metric.domain.dih\" " 
        + "value=\"ref:pengyun-instancehub|metric.properties|metric.prefix\"/>"
        + "<property name=\"metric.domain.monitorcenter\" " 
        + "value=\"ref:pengyun-system_monitor|metric.properties|metric.prefix\"/>"
        + "</file>" + "<file name=\"metric.properties\">"
        + "<property name=\"metric.prefix\" value=\"pengyun.monitorcenter\" />" + "</file>"
        + "<file name=\"log4j.properties\">"
        + "<property name=\"log4j.rootLogger\" value=\"DEBUG, FILE\"/>"
        + "</file>" + "<file name=\"jmxagent.properties\">"
        + "<property name=\"jmx.agent.port\" value=\"11100\"/>"
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>" + "</file>"
        + "</project>"
        + "</configurations>";
  }
}
