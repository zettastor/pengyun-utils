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

package py.deployment.client;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import py.DeploymentDaemonClientFactory;
import py.common.PyService;
import py.dd.DeploymentDaemonClientWrapper;
import py.dd.common.ServiceMetadata;
import py.dd.common.ServiceStatus;
import py.deployment.common.DeploymentConfigurationFactory;
import py.test.TestBase;

/**
 * xx.
 */
public class ErrorServiceStatusTest extends TestBase {

  private static final Logger logger = LoggerFactory
      .getLogger(DeploymentConfigurationFactory.class);
  @Mock
  DeploymentConfigurationFactory configurationFactory;
  @Mock
  DeploymentDaemonClientFactory ddClientFactory;
  @Mock
  DeploymentDaemonClientWrapper ddClient;
  private DeploymentImpl deploymentImpl;
  private ServiceMetadata serviceMetadata;
  private List<String> hostList;

  /**
   * xx.
   */
  public void init() throws Exception {
    super.init();
    super.setLogLevel(Level.ALL);
    deploymentImpl = new DeploymentImpl(configurationFactory, ddClientFactory);
    serviceMetadata = new ServiceMetadata();
    serviceMetadata.setServiceName(PyService.DRIVERCONTAINER.getServiceName());
    serviceMetadata.setServiceStatus(ServiceStatus.ERROR);
    hostList = new ArrayList<String>();
    hostList.add("192.168.2.101");
    hostList.add("192.168.2.102");
    hostList.add("192.168.2.103");
    when(ddClientFactory.build(any(String.class), anyInt())).thenReturn(ddClient);
    when(ddClient.checkService(any(String.class))).thenReturn(serviceMetadata);
  }

  @Test
  public void serviceStatusIsErrorTest() {
    List<String> pendingHosts = deploymentImpl
        .waitUntilServiceOnAllHostsTurnInto(serviceMetadata.getServiceName(),
            ServiceStatus.DEACTIVE, hostList,
            9000, 5000);
    Assert.assertTrue(pendingHosts.size() == 3);

  }
}
