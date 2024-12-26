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

package py.client.dd;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import org.apache.log4j.Level;
import org.apache.thrift.TException;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import py.DeploymentDaemonClientFactory;
import py.common.struct.EndPoint;
import py.dd.DeploymentDaemonClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.test.TestBase;
import py.thrift.deploymentdaemon.DeploymentDaemon;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;
import py.thrift.share.SetConfigurationsResponse;

/**
 *xx.
 */
public class DeploymentDamonClientTest extends TestBase {

  @Mock
  private DeploymentDaemonClientFactory ddClientFactory;
  @Mock
  private DeploymentDaemonClientWrapper ddClient;
  @Mock
  private DeploymentDaemon.Iface iface;

  private SetConfigurationsResponse setconfigresponse = new SetConfigurationsResponse();

  private GetConfigurationsResponse getconfigresponse = new GetConfigurationsResponse();

  private DeploymentDaemon.Iface iface1;

  @Override
  public void init() throws Exception {
    super.init();

    super.setLogLevel(Level.DEBUG);
  }

  @Ignore
  @Test
  public void test() throws GenericThriftClientFactoryException, TException {
    when(ddClientFactory.build(any(EndPoint.class))).thenReturn(ddClient);
    when(ddClient.getClient()).thenReturn(iface);
    when(iface.setConfigurations(any(SetConfigurationsRequest.class)))
        .thenReturn(setconfigresponse);
    when(iface.getConfigurations(any(GetConfigurationsRequest.class)))
        .thenReturn(getconfigresponse);

    String host = "225.225.225.225";
    String port = "10005";

  }

}
