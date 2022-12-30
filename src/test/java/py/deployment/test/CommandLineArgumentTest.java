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

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import org.testng.Assert;
import py.common.PyService;
import py.deployment.client.DeploymentCommandLineArgument;
import py.deployment.common.DeploymentOperation;
import py.instance.Group;

/**
 * A test includes some test for command line argument parsing.
 */
public class CommandLineArgumentTest {

  @Test
  public void testCommandLineArgumentInitialization() {
    String[] args = {"--operation=deploy", "--serviceName=ALL",
        "--serviceHostRange=10.0.1.1:10.0.1.3",
        "--params=test", "--groupId=1"};

    PropertySource propertySource = new SimpleCommandLinePropertySource(args);
    AnnotationConfigApplicationContext commandLineContext =
        new AnnotationConfigApplicationContext();
    commandLineContext.getEnvironment().getPropertySources().addFirst(propertySource);
    commandLineContext.register(DeploymentCommandLineArgument.class);
    commandLineContext.refresh();

    DeploymentCommandLineArgument commandLine = commandLineContext
        .getBean(DeploymentCommandLineArgument.class);

    Assert.assertTrue(commandLine.getOperation() == DeploymentOperation.DEPLOY);
    Assert.assertTrue(commandLine.getGroup().equals(new Group(1)));
    Assert.assertTrue(commandLine.getParams().get(0).equals("test"));
    Assert.assertTrue(commandLine.getServiceHostRange().equals("10.0.1.1:10.0.1.3"));
    for (PyService pyServiceName : PyService.values()) {
      if (pyServiceName != PyService.DEPLOYMENTDAMON) {

        Assert
            .assertTrue(commandLine.getServiceNameList().contains(pyServiceName.getServiceName()));
      }
    }
  }
}