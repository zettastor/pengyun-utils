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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.Assert;
import py.common.PyService;
import py.deployment.client.DeploymentAppBeans;
import py.deployment.common.DeploymentConfigurationFactory;
import py.test.TestBase;

/**
 * A class includes some test for deployment app beans.
 */
public class DeploymentAppBeansTest extends TestBase {

  private static final Logger logger = LoggerFactory.getLogger(DeploymentAppBeansTest.class);

  @Override
  public void init() throws Exception {
    // TODO Auto-generated method stub
    super.init();
  }

  @Test
  public void testDeploymentAppBeansInitializationWithoutNoException() {
    ApplicationContext appContex = new AnnotationConfigApplicationContext(DeploymentAppBeans.class);
    logger.warn("test begin ... ");
    // check if service deployment configuration is complete
    DeploymentConfigurationFactory deploymentConfigurationFactory = appContex
        .getBean(DeploymentConfigurationFactory.class);
    for (PyService pyServiceName : PyService.values()) {
      if (pyServiceName != PyService.DEPLOYMENTDAMON) {
        logger.warn("server: {}", pyServiceName);
        Assert.assertNotNull(
            deploymentConfigurationFactory
                .getDeploymentConfiguration(pyServiceName.getServiceName()));
      }
    }
  }
}
