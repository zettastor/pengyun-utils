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
