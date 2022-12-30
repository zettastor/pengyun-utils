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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.Assert;
import py.DeploymentDaemonClientFactory;
import py.common.PyService;
import py.common.struct.EndPoint;
import py.dd.DeploymentDaemonClientWrapper;
import py.dd.common.ServiceMetadata;
import py.dd.common.ServiceStatus;
import py.deployment.client.DeploymentImpl;
import py.deployment.common.DeploymentConfiguration.DataNodeDeploymentConfiguration;
import py.deployment.common.DeploymentConfiguration.DihDeploymentConfiguration;
import py.deployment.common.DeploymentConfigurationFactory;
import py.deployment.common.DeploymentOperation;
import py.deployment.common.DeploymentScene;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.PortType;
import py.test.TestBase;

/**
 * A class includes some test for deployment.
 */
public class DeploymentTest extends TestBase {

  @Mock
  private DeploymentDaemonClientFactory ddClientFactory;
  @Mock
  private DeploymentDaemonClientWrapper ddClient;

  @Mock
  private DeploymentConfigurationFactory configFactory;

  @Mock
  private DihClientFactory dihClientFactory;
  @Mock
  private DihServiceBlockingClientWrapper dihClient;

  private DeploymentImpl deployment;

  /**
   * xx.
   */
  @Before
  public void init() throws Exception {
    super.init();

    deployment = new DeploymentImpl(configFactory, ddClientFactory);
    deployment.setDihClientFactory(dihClientFactory);
  }

  /**
   * Test succeed to activate dih service.
   */
  @Test
  public void testActivatingDihService() throws Exception {

    ServiceMetadata service = new ServiceMetadata();
    service.setServiceStatus(ServiceStatus.ACTIVE);

    String[] centerdihhosts = {"10.0.1.1"};
    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    DihDeploymentConfiguration config = new DihDeploymentConfiguration();
    config.setCenterDihHosts(Arrays.asList(centerdihhosts));
    config.setServiceDeploymentHosts(Arrays.asList(dihHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(10000);

    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(config);
    when(dihClientFactory.build(any(EndPoint.class))).thenReturn(dihClient);
    when(ddClientFactory.build(any(String.class), anyInt())).thenReturn(ddClient);
    when(ddClientFactory.build(any(String.class), anyInt(), anyLong())).thenReturn(ddClient);
    when(ddClient.checkService(PyService.DIH.getServiceName())).thenReturn(service);

    deployment.init(PyService.DIH.getServiceName(), 0);
    Assert.assertTrue(deployment.activate());

    Mockito.verify(ddClient, Mockito.times(4)).checkService(PyService.DIH.getServiceName());
    Mockito.verify(ddClient, Mockito.times(4))
        .activate(PyService.DIH.getServiceName(), "2.3.0-internal");
  }

  /**
   * Test timeout to activate dih.
   */
  @Test
  public void testTimeoutActivatingDihService() throws Exception {
    ServiceMetadata service = new ServiceMetadata();
    service.setServiceStatus(ServiceStatus.ACTIVATING);

    String[] centerDihHosts = {"10.0.1.1"};
    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    DihDeploymentConfiguration config = new DihDeploymentConfiguration();
    config.setCenterDihHosts(Arrays.asList(centerDihHosts));
    config.setServiceDeploymentHosts(Arrays.asList(dihHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(5000);

    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(config);
    when(dihClientFactory.build(any(EndPoint.class))).thenReturn(dihClient);
    when(ddClientFactory.build(any(String.class), anyInt())).thenReturn(ddClient);
    when(ddClient.checkService(PyService.DIH.getServiceName())).thenReturn(service);

    deployment.init(PyService.DIH.getServiceName(), 0);
    Assert.assertFalse(deployment.activate());
  }

  @Test
  public void testUpgradeDatanodeWithGroup() throws Exception {
    Path packagePath = Paths.get("/tmp/pengyun-instancehub-2.3.0-internal.tar.gz");
    if (packagePath.toFile().exists()) {
      packagePath.toFile().delete();
    }

    final byte[] byteBuf = new byte[1024];
    for (int i = 0; i < byteBuf.length; i++) {
      byteBuf[i] = (byte) (i % Byte.MAX_VALUE);
    }
    FileOutputStream outStream = new FileOutputStream(packagePath.toFile());
    outStream.write(byteBuf);
    outStream.write(byteBuf);
    outStream.close();

    ServiceMetadata serviceWithStatusDeactive = new ServiceMetadata();
    serviceWithStatusDeactive.setServiceStatus(ServiceStatus.DEACTIVE);

    ServiceMetadata serviceWithStatusActive = new ServiceMetadata();
    serviceWithStatusActive.setServiceStatus(ServiceStatus.ACTIVE);

    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};
    String[] datanodeHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    Set<Instance> datanodeInstances = new HashSet<Instance>();
    Instance instance = new Instance(new InstanceId(1), new Group(1),
        PyService.DATANODE.getServiceName(),
        InstanceStatus.HEALTHY);
    instance.putEndPointByServiceName(PortType.CONTROL, new EndPoint(datanodeHosts[1], 10011));
    datanodeInstances.add(instance);

    DihDeploymentConfiguration dihConfig = new DihDeploymentConfiguration();
    dihConfig.setServiceDeploymentHosts(Arrays.asList(dihHosts));

    File datanodePackage = new File("/tmp/pengyun-datanode-2.3.0-internal.tar.gz");
    datanodePackage.createNewFile();

    DataNodeDeploymentConfiguration config = new DataNodeDeploymentConfiguration();
    config.setPackagesPath(Paths.get("/tmp"));
    config.setServiceDeploymentHosts(Arrays.asList(datanodeHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(10000);
    config.setGroup(new Group(1));
    config.setOperationTimeoutMs(20000);
    config.setDeploymentThreadAmount(1);

    when(configFactory.getDeploymentConfiguration(PyService.DATANODE.getServiceName()))
        .thenReturn(config);
    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(dihConfig);
    when(dihClientFactory.build(any(EndPoint.class))).thenReturn(dihClient);
    when(dihClient.getInstanceInGroup(any(Group.class))).thenReturn(datanodeInstances);
    when(ddClientFactory.build(any(String.class), anyInt(), anyLong())).thenReturn(ddClient);
    when(ddClientFactory.build(any(String.class), anyInt())).thenReturn(ddClient);
    when(ddClient.checkService(PyService.DATANODE.getServiceName()))
        .thenReturn(serviceWithStatusDeactive)
        .thenReturn(serviceWithStatusDeactive).thenReturn(serviceWithStatusDeactive)
        .thenReturn(serviceWithStatusActive);

    deployment.init(PyService.DATANODE.getServiceName(), 0);
    Assert.assertTrue(deployment.upgrade());

    Mockito.verify(ddClient, Mockito.times(1)).deactivate(PyService.DATANODE.getServiceName(), 0);
    Mockito.verify(ddClient, Mockito.times(1))
        .transferPackage(eq(PyService.DATANODE.getServiceName()),
            eq("2.3.0-internal"), any(ByteBuffer.class));
    Mockito.verify(ddClient, Mockito.times(1)).activate(eq(PyService.DATANODE.getServiceName()),
        eq("2.3.0-internal"));
  }


  @Test
  public void transferPackageConcurrently() throws Exception {
    String[] centerDihHosts = {"10.0.1.1"};
    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    Path dihPackagePath = Paths.get("/tmp/pengyun-instancehub-2.3.0-internal.tar.gz");
    dihPackagePath.toFile().createNewFile();

    DihDeploymentConfiguration config = new DihDeploymentConfiguration();
    config.setCenterDihHosts(Arrays.asList(centerDihHosts));
    config.setServiceDeploymentHosts(Arrays.asList(dihHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(10000);
    config.setDeploymentThreadAmount(10);
    config.setPackagesPath(dihPackagePath.getParent());

    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(config);
    when(dihClientFactory.build(any(EndPoint.class))).thenReturn(dihClient);
    when(ddClientFactory.build(any(String.class), anyInt())).thenReturn(ddClient);
    when(ddClientFactory.build(any(String.class), anyInt(), anyLong())).thenReturn(ddClient);

    deployment.init(PyService.DIH.getServiceName(), 0);
    Assert.assertTrue(deployment.transfer());

    deployment.init(PyService.DIH.getServiceName(), 0);
    config.setDeploymentThreadAmount(2);
    Assert.assertTrue(deployment.transfer());
  }

  @Test
  public void keepDeploymentWhenOccurErr() throws Exception {
    ServiceMetadata service = new ServiceMetadata();
    service.setServiceStatus(ServiceStatus.ACTIVE);

    String[] centerDihHosts = {"10.0.1.1"};
    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    Path dihPackagePath = Paths.get("/tmp/pengyun-instancehub-2.3.0-internal.tar.gz");
    dihPackagePath.toFile().createNewFile();

    DihDeploymentConfiguration config = new DihDeploymentConfiguration();
    config.setCenterDihHosts(Arrays.asList(centerDihHosts));
    config.setServiceDeploymentHosts(Arrays.asList(dihHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(10000);
    config.setDeploymentThreadAmount(10);
    config.setPackagesPath(dihPackagePath.getParent());

    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(config);
    when(ddClientFactory.build(eq("10.0.1.2"), anyInt(), anyLong()))
        .thenThrow(new GenericThriftClientFactoryException());
    when(ddClientFactory.build(eq("10.0.1.2"), anyInt()))
        .thenThrow(new GenericThriftClientFactoryException());
    when(ddClientFactory.build(eq("10.0.1.1"), anyInt(), anyLong())).thenReturn(ddClient);
    when(ddClientFactory.build(eq("10.0.1.3"), anyInt(), anyLong())).thenReturn(ddClient);
    when(ddClientFactory.build(eq("10.0.1.1"), anyInt())).thenReturn(ddClient);
    when(ddClientFactory.build(eq("10.0.1.3"), anyInt())).thenReturn(ddClient);
    when(ddClient.checkService(PyService.DIH.getServiceName())).thenReturn(service);

    deployment.init(PyService.DIH.getServiceName(), 0);
    Assert.assertFalse(deployment.deploy());

    Mockito.verify(ddClient, Mockito.times(2)).transferPackage(any(String.class), any(String.class),
        any(ByteBuffer.class));
    Mockito.verify(ddClient, Mockito.times(3)).activate(any(String.class), any(String.class));

    Assert.assertEquals(deployment.getFailures().get(DeploymentOperation.TRANSFER).get(0),
        "10.0.1.2");
  }

  @Test
  public void discardDihActivationWhenCenterErr() throws Exception {
    String[] centerDihHosts = {"10.0.1.1"};
    String[] dihHosts = {"10.0.1.1", "10.0.1.2", "10.0.1.3"};

    Path dihPackagePath = Paths.get("/tmp/pengyun-instancehub-2.3.0-internal.tar.gz");
    dihPackagePath.toFile().createNewFile();

    DihDeploymentConfiguration config = new DihDeploymentConfiguration();
    config.setCenterDihHosts(Arrays.asList(centerDihHosts));
    config.setServiceDeploymentHosts(Arrays.asList(dihHosts));
    config.setServiceVersion("2.3.0");
    config.setScene(DeploymentScene.INTERNAL.getValue());
    config.setOperationTimeoutMs(10000);
    config.setDeploymentThreadAmount(10);
    config.setPackagesPath(dihPackagePath.getParent());

    when(configFactory.getDeploymentConfiguration(PyService.DIH.getServiceName()))
        .thenReturn(config);
    when(ddClientFactory.build(eq("10.0.1.1"), anyInt()))
        .thenThrow(new GenericThriftClientFactoryException());
    when(ddClientFactory.build(eq("10.0.1.2"), anyInt())).thenReturn(ddClient);
    when(ddClientFactory.build(eq("10.0.1.3"), anyInt())).thenReturn(ddClient);

    deployment.init(PyService.DIH.getServiceName(), 0);
    Assert.assertFalse(deployment.activate());

    Assert.assertEquals(deployment.getFailures().size(), 1);
    Assert.assertEquals(deployment.getFailures().get(DeploymentOperation.ACTIVATE).size(), 3);
    Assert.assertEquals(deployment.getFailures().get(DeploymentOperation.ACTIVATE).get(0),
        "10.0.1.1");
  }

  /**
   * In this test plan, it throws exception on DIH building client. And expect retry some times.
   */
  @Test
  public void testPickServiceHostByGroupFails() throws Exception {
    final Group group = new Group(0);

    List<String> serviceHosts;
    DataNodeDeploymentConfiguration datanodeDeploymentConfiguration;

    serviceHosts = new ArrayList<>();
    serviceHosts.add("255.255.255.255");

    datanodeDeploymentConfiguration = Mockito.mock(DataNodeDeploymentConfiguration.class);
    DihDeploymentConfiguration dihDeploymentConfiguration
        = Mockito.mock(DihDeploymentConfiguration.class);

    Mockito.when(datanodeDeploymentConfiguration.getGroup()).thenReturn(group);
    Mockito.when(datanodeDeploymentConfiguration.isGroupEnabled()).thenReturn(false);
    Mockito.when(dihDeploymentConfiguration.getServiceDeploymentHosts()).thenReturn(serviceHosts);
    Mockito.when(configFactory
        .getDeploymentConfiguration(Mockito.contains(PyService.DATANODE.getServiceName())))
        .thenReturn(datanodeDeploymentConfiguration);
    Mockito.when(
        configFactory.getDeploymentConfiguration(Mockito.contains(PyService.DIH.getServiceName())))
        .thenReturn(dihDeploymentConfiguration);

    Mockito.when(dihClientFactory.build(Mockito.any(EndPoint.class)))
        .thenThrow(new GenericThriftClientFactoryException());

    ((DeploymentImpl) deployment)
        .pickServiceHostByGroup(PyService.DATANODE.getServiceName(), new ArrayList<>());
    Mockito.verify(dihClientFactory, Mockito.times(3)).build(Mockito.any(EndPoint.class));
  }
}
