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
