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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.test.TestBase;

/**
 * A class includes some tests for {@link XmlConfigurationFileReader}.
 */
public class XmlConfigurationReaderTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(XmlConfigurationReaderTest.class);
  protected File testXmlFile;
  private XmlConfigurationFileReader xmlReader;

  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();

    testXmlFile = new File("/tmp/test.xml");
    if (testXmlFile.exists()) {
      testXmlFile.delete();
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(testXmlFile));
    // bw.write(xmlBuilder.toString());
    bw.write(getTestingXml());
    bw.flush();
    bw.close();

    xmlReader = new XmlConfigurationFileReader(Paths.get(testXmlFile.getAbsolutePath()));
  }

  @Test
  public void testXmlConfigurationReader() {
    logger.debug("{}", xmlReader);
    Map<String, Properties> controlcenterProperties = xmlReader
        .getProperties("pengyun-controlcenter");
    for (String fileName : controlcenterProperties.keySet()) {
      logger.debug("controlcenter properties in file {}: \n{}", fileName,
          controlcenterProperties.get(fileName));
    }

    Assert.assertEquals(controlcenterProperties.get("metric.properties").get("metric.prefix"),
        "pengyun.integration_test.controlcenter");
    Assert.assertEquals(controlcenterProperties.get("log4j.properties").get("log4j.rootLogger"),
        "WARN,  FILE");

    Map<String, Properties> monitorcenterPorperties = xmlReader
        .getProperties("pengyun-system_monitor");
    for (String fileName : monitorcenterPorperties.keySet()) {
      logger.debug("systemmonitor properties in file {}: \n{}", fileName,
          monitorcenterPorperties.get(fileName));
    }
    Assert.assertEquals(monitorcenterPorperties.get("jmxagent.properties").get("jmx.agent.port"),
        "11100");
    Assert.assertEquals(
        monitorcenterPorperties.get("systemmonitor.properties").get("jmx.agent.port.controlcenter"),
        "8110");
  }

  @Test
  public void testsetseet() {
    String test = "ref:pengyun-controlcenter|log4j.properties|jmx.agent.port";
    Vector<String> results = xmlReader.parseReference(test);
    logger.debug("{}", results);
    Assert.assertEquals(results.get(0), "pengyun-controlcenter");
    Assert.assertEquals(results.get(1), "log4j.properties");
    Assert.assertEquals(results.get(2), "jmx.agent.port");

    boolean isReference = xmlReader.isReference(test);
    Assert.assertTrue(isReference);
  }

  protected String getTestingXml() {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<configurations>"
        + "    <project name=\"*\">"
        + "        <file name=\"storage.properties\">"
        + "            <property name=\"page.size.byte\" value=\"65536\" />"
        + "            <!-- 1G -->"
        + "            <property name=\"segment.size.byte\" value=\"1073741824\" />"
        + "        </file>"
        + "               <file name=\"network.properties\">"
        + "                        <property name=\"control.flow.subnet\" value=\"10.0.1.0/24\" />"
        + "               </file>"
        + "               <file name=\"log4j.properties\">"
        + "                       <property name=\"log4j.rootLogger\" value=\"WARN, FILE\"/>"
        + "               </file>"
        + "               <file name=\"metric.properties\">"
        + "                       <property name=\"metric.enable.profiles\" value=\"metric.jmx\"" 
        + " range=\"metric.jmx;metric.graphite;metric.csv;metric.console;metri.slf4j\" />"
        + "                       <!--<property name=\"metric.enable.profiles\" " 
        + "value=\"metric.graphite\" range=\"metric.jmx;metric.graphite;" 
        + "metric.csv;metric.console;metri.slf4j\" />"
        + "                       <property name=\"metric.graphite.ip\" value=\"10.0.1.205\" />-->"
        + "               </file>"
        + "    </project>"
        + ""
        + "    <project name=\"pengyun-console\">"
        + "        <file name=\"console.properties\">"
        + "        </file>"
        + "    </project>"
        + ""
        + "    <project name=\"pengyun-controlcenter\">"
        + "        <file name=\"controlcenter.properties\">"
        + "            <property name=\"zookeeper.election.switch\" value=\"true\" " 
        + "range=\"true;false\"/>"
        + "            <property name=\"jdbc.driver.class\" value=\"org.postgresql.Driver\" />"
        + "            <property name=\"jdbc.url\" " 
        + "value=\"jdbc:postgresql://10.0.1.157:5432/controlcenterdb\" />"
        + "            <property name=\"jdbc.initial.pool.size\" value=\"5\" />"
        + "            <property name=\"jdbc.min.pool.size\" value=\"5\" />"
        + "            <property name=\"jdbc.max.pool.size\" value=\"30\" />"
        + "                        <!-- use zookeeper store in case of integretion test start-->"
        + "                        <property name=\"license.storage.type\" " 
        + "value=\"zookeeper\" range=\"file;zookeeper\"/>"
        + "        </file>"
        + "                <file name=\"metric.properties\">"
        + "                        <property name=\"metric.prefix\" " 
        + "value=\"pengyun.integration_test.controlcenter\" />"
        + "                </file>"
        + "                <file name=\"log4j.properties\">"
        + "                        <property name=\"log4j.rootLogger\" value=\"WARN,  FILE\"/>"
        + "                </file>"
        + "                <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"8110\"/>"
        + "                        <property name=\"jmx.agent.switcher\" " 
        + "value=\"on\" range=\"on;off\"/>"
        + "                </file>"
        + "    </project>"
        + "    <project name=\"pengyun-coordinator\">"
        + "                <file name=\"coordinator-log4j.properties\">"
        + "                        <property name=\"log4j.rootLogger\" value=\"DEBUG, FILE\"/>"
        + "                </file>"
        + "                <file name=\"coordinator.properties\">"
        + "        </file>"
        + "        <file name=\"coordinator-jvm.properties\">"
        + "            <property name=\"initial.mem.pool.size\" value=\"1024m\"/>"
        + "                <property name=\"min.mem.pool.size\" value=\"1024m\"/>"
        + "            <property name=\"max.mem.pool.size\" value=\"1024m\"/>"
        + "        </file>"
        + "        <file name=\"drivercontainer.properties\">"
        + "        </file>"
        + "                <file name=\"metric.properties\">"
        + "                        <property name=\"metric.prefix\" " 
        + "value=\"pengyun.integration_test.coordinator\" />"
        + "                </file>"
        + "         <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"8110\"/>"
        + "                        <property name=\"jmx.agent.switcher\"" 
        + " value=\"on\" range=\"on;off\"/>"
        + "         </file>"
        + "    </project>"
        + "    <project name=\"pengyun-datanode\">"
        + "        <file name=\"datanode.properties\">"
        + "                        <property name=\"janitor.execution.rate.ms\" value=\"60000\" />"
        + "                        <property " 
        + "name=\"request.new.segment.unit.expiration.threshold.ms\" value=\"80000\"/>"
        + "                        <property" 
        + " name=\"threshold.to.remove.inactive.secondary.ms\" value=\"120000\" />"
        + "                        <property " 
        + "name=\"wait.time.ms.to.move.segment.to.deleted\" value=\"60000\" />"
        + "                        <property name=\"memory.size.for.data.logs.mb\" value=\"100\" />"
        + "                        <property name=\"file.buffer.size.gb\" value=\"1\" />"
        + "                        <property name=\"enable.ssd.for.data.log\" value=\"false\" />"
        + "                        <property name=\"enable.file.buffer\" value=\"false\" />"
        + "        </file>"
        + "                <file name=\"metric.properties\">"
        + "                        <property " 
        + "name=\"metric.prefix\" value=\"pengyun.integration_test.datanode\" />"
        + "                </file>"
        + "                <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"10111\"/>"
        + "                        <property" 
        + " name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>"
        + "                </file>"
        + "    </project>"
        + "    <project name=\"pengyun-deployment_daemon\">"
        + "        <file name=\"deployment_daemon.properties\">"
        + "        </file>"
        + "    </project>"
        + "    <project name=\"pengyun-infocenter\">"
        + "        <file name=\"infocenter.properties\">"
        + "            <property " 
        + "name=\"zookeeper.election.switch\" value=\"true\" range=\"true;false\"/>"
        + "            <property name=\"dead.volume.to.remove.second\" value=\"300\" />"
        + "            <property name=\"jdbc.driver.class\" value=\"org.postgresql.Driver\" />"
        + "            <property " 
        + "name=\"jdbc.url\" value=\"jdbc:postgresql://10.0.1.157:5432/infocenterdb\" />"
        + "            <property name=\"jdbc.initial.pool.size\" value=\"5\" />"
        + "            <property name=\"jdbc.min.pool.size\" value=\"5\" />"
        + "                        " 
        + "<property name=\"instance.metadata.to.remove\" value=\"30000\" />"
        + "                       <property name=\"jdbc.max.pool.size\" value=\"30\" />"
        + "                </file>"
        + "                <file name=\"metric.properties\">"
        + "                      " 
        + " <property name=\"metric.prefix\" value=\"pengyun.integration_test.infocenter\" />"
        + "                </file>"
        + "                <file name=\"log4j.properties\">"
        + "                       <property name=\"log4j.rootLogger\" value=\"WARN, FILE\"/>"
        + "                </file>"
        + "                <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"8120\"/>"
        + "                        " 
        + "<property name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>"
        + "                </file>"
        + "    </project>"
        + "    <project name=\"pengyun-instancehub\">"
        + "        <file name=\"instancehub.properties\">"
        + "            <property name=\"center.dih.endpoint\" value=\"10.0.1.152:10000\" />"
        + "                        <property name=\"time.ok.to.inc\" value=\"5000\" />"
        + "                        " 
        + "<property name=\"time.failed.to.forgotten.of.local\" value=\"10000\" />"
        + "                        <property name=\"time.failed.to.forgotten\" value=\"10000\" />"
        + "                        " 
        + "<property name=\"time.forgotten.to.remove.of.local\" value=\"10000\" />"
        + "                        <property name=\"time.forgotten.to.remove\" value=\"10000\" />"
        + "        </file>"
        + "        <file name=\"metric.properties\">"
        + "                        <property" 
        + " name=\"metric.prefix\" value=\"pengyun.integration_test.instancehub\" />"
        + "        </file>"
        + "        <file name=\"log4j.properties\">"
        + "                        <property name=\"log4j.rootLogger\" value=\"DEBUG, FILE\"/>"
        + "        </file>"
        + "        <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"10100\"/>"
        + "                        <property" 
        + " name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>"
        + "        </file>"
        + "    </project>"
        + "    <project name=\"pengyun-system_monitor\">"
        + "        <file name=\"systemmonitor.properties\">"
        + "                        <property" 
        + " name=\"jmx.agent.port.inforcenter\" value=\"" 
        + "ref:pengyun-infocenter|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property " 
        + "name=\"jmx.agent.port.controlcenter\" " 
        + "value=\"ref:pengyun-controlcenter|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property " 
        + "name=\"jmx.agent.port.drivercontainer\" " 
        + "value=\"ref:pengyun-coordinator|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property " 
        + "name=\"jmx.agent.port.datanode\" " 
        + "value=\"ref:pengyun-datanode|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property " 
        + "name=\"jmx.agent.port.dih\" value=\"" 
        + "ref:pengyun-instancehub|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property " 
        + "name=\"jmx.agent.port.systemmonitor\" " 
        + "value=\"ref:pengyun-system_monitor|jmxagent.properties|jmx.agent.port\" />"
        + "                        <property name=\"metric.domain.infocenter\">"
        + "                               " 
        + "<value sub_value=\"ref:pengyun-infocenter|metric.properties|metric.prefix\"/>"
        + "                               <value sub_value=\"123\"/>"
        + "                               <value sub_value=\"456\"/>"
        + "                        </property>"
        + "        </file>"
        + "        <file name=\"metric.properties\">"
        + "                        <property" 
        + " name=\"metric.prefix\" value=\"pengyun.systemmonitor\" />"
        + "        </file>"
        + "        <file name=\"log4j.properties\">"
        + "                        <property name=\"log4j.rootLogger\" value=\"DEBUG, FILE\"/>"
        + "        </file>"
        + "        <file name=\"jmxagent.properties\">"
        + "                        <property name=\"jmx.agent.port\" value=\"11100\"/>"
        + "                        <property" 
        + " name=\"jmx.agent.switcher\" value=\"on\" range=\"on;off\"/>"
        + "        </file>"
        + "    </project>"
        + "</configurations>";
  }

  @Test
  public void testXmlReadProperties() {
    Map<String, Properties> propsFileName2Properties = xmlReader
        .getProperties("pengyun-instancehub");
    ;
    Properties properties = propsFileName2Properties.get("instancehub.properties");
    String xmlcenterdihhostrange = null;
    List<String> xmlcenterdihhosts = new ArrayList<>();

    for (Object key : properties.keySet()) {
      if (((String) key).equals("center.dih.endpoint")) {
        xmlcenterdihhostrange = properties.getProperty((String) key);
      }
    }
    logger.debug("xmlcenterdihhostrange {}", xmlcenterdihhostrange);

    assert (xmlcenterdihhostrange.equals("10.0.1.152:10000"));
    return;
  }

}
