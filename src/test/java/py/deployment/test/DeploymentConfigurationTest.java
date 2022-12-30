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

package py.deployment.test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;
import py.deployment.common.DeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;

/**
 * A class includes some test for deployment configuration.
 */
public class DeploymentConfigurationTest {

  /**
   * Create some valid and invalid host range to test the method.
   */
  @Test
  public void testGettingAllHostsInRange() throws UnknownHostException {
    DeploymentConfiguration config = new DeploymentConfiguration();
    config.setSubNetwork("10.0.1.1/24");

    String validHostRange1 = "10.0.1.1";
    String validHostRange2 = "10.0.1.1:10.0.1.3";
    String validHostRange3 = "10.0.1.1:10.0.1.3,10.0.1.5";
    String validHostRange4 = "10.0.1.1:10.0.1.3,10.0.1.5:10.0.1.6";
    String validHostRange5 = "10.0.1.3:10.0.1.1";

    final String[] hostsInValidHostRange1 = {"10.0.1.1"};
    final String[] hostsInValidHostRange2 = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};
    final String[] hostsInValidHostRange3 = {"10.0.1.1", "10.0.1.2", "10.0.1.3", "10.0.1.5"};
    final String[] hostsInValidHostRange4 = {"10.0.1.1", "10.0.1.2", "10.0.1.3", 
        "10.0.1.5", "10.0.1.6"};

    List<String> hostListInValidHostRange1 = config.getAllHostsInRange(validHostRange1);
    List<String> hostListInValidHostRange2 = config.getAllHostsInRange(validHostRange2);
    List<String> hostListInValidHostRange3 = config.getAllHostsInRange(validHostRange3);
    List<String> hostListInValidHostRange4 = config.getAllHostsInRange(validHostRange4);
    List<String> hostListInValidHostRange5 = config.getAllHostsInRange(validHostRange5);

    Collections.sort(hostListInValidHostRange1);
    Collections.sort(hostListInValidHostRange2);
    Collections.sort(hostListInValidHostRange3);
    Collections.sort(hostListInValidHostRange4);
    Collections.sort(hostListInValidHostRange5);

    Assert.assertTrue(hostListInValidHostRange1.equals(Arrays.asList(hostsInValidHostRange1)));
    Assert.assertTrue(hostListInValidHostRange2.equals(Arrays.asList(hostsInValidHostRange2)));
    Assert.assertTrue(hostListInValidHostRange3.equals(Arrays.asList(hostsInValidHostRange3)));
    Assert.assertTrue(hostListInValidHostRange4.equals(Arrays.asList(hostsInValidHostRange4)));
    Assert.assertTrue(hostListInValidHostRange5.equals(Arrays.asList(hostsInValidHostRange2)));

    String invalidHostRange1 = "10.1.1.1";
    String invalidHostRange2 = "10.0.1.1::10.0.1.3";
    String invalidHostRange3 = "10.0.1.1:?10.0.1.3";

    boolean exceptionCached = false;
    try {
      config.getAllHostsInRange(invalidHostRange1);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);

    exceptionCached = false;

    try {
      config.getAllHostsInRange(invalidHostRange2);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);

    exceptionCached = false;

    try {
      config.getAllHostsInRange(invalidHostRange3);
    } catch (Exception e) {
      exceptionCached = true;
    }

    Assert.assertTrue(exceptionCached);
  }

  @Test
  public void largeSubnet() throws UnknownHostException {
    DeploymentConfiguration config = new DeploymentConfiguration();
    config.setSubNetwork("1.0.0.0/0");

    String validHostRange1 = "10.0.1.1";
    String validHostRange2 = "10.0.1.1:10.0.1.3";

    String[] hostsInValidHostRange1 = {"10.0.1.1"};
    String[] hostsInValidHostRange2 = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    List<String> hostListInValidHostRange1 = config.getAllHostsInRange(validHostRange1);
    List<String> hostListInValidHostRange2 = config.getAllHostsInRange(validHostRange2);

    Assert.assertTrue(hostListInValidHostRange1.equals(Arrays.asList(hostsInValidHostRange1)));
    Assert.assertTrue(hostListInValidHostRange2.equals(Arrays.asList(hostsInValidHostRange2)));
  }

  @Test
  public void parseLargeNumberOfIps() throws Exception {
    int ipAmount = 10000;
    StringBuilder iplistsb = new StringBuilder();
    Random ipRandom = new Random(System.currentTimeMillis());
    DeploymentConfiguration config = new DeploymentConfiguration();

    for (int i = 0; i < ipAmount; i++) {
      StringBuilder ipsb = new StringBuilder();
      for (int j = 0; j < 4; j++) {
        ipsb.append(ipRandom.nextInt(256));
        ipsb.append('.');
      }

      iplistsb.append(ipsb.substring(0, ipsb.length() - 1).toString());
      iplistsb.append(',');
    }

    config.setSubNetwork("0.0.0.0/0");
    config.getAllHostsInRange(iplistsb.toString());

    config.setSubNetwork("10.0.0.0/16");
    config.getAllHostsInRange("10.0.1.1:10.0.255.255");

    config.setSubNetwork("255.0.0.0/16");
    config.getAllHostsInRange("255.0.1.1:255.0.255.255");
  }

  @Test
  public void testParseGroupNum() throws Exception {
    String validKey = "DataNode.deployment.host.group.2";
    String invalidKey = "DataNode.deployment.host.group.enabled";

    DataNodeDeploymentConfiguration dataNodeDeploymentConfiguration =
        new DataNodeDeploymentConfiguration();
    Assert.assertEquals(2, dataNodeDeploymentConfiguration.parseGroupNum(validKey));
    Assert.assertEquals(-1, dataNodeDeploymentConfiguration.parseGroupNum(invalidKey));
  }
}
