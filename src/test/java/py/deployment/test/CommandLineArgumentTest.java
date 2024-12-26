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