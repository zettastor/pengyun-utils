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

package py.memorynbdserver.mocktools;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentVersion;
import py.common.PyService;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.lib.Coordinator;
import py.coordinator.lib.StorageDriver;
import py.coordinator.volumeinfo.DummyVolumeInfoRetriever;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.DummyInstanceStore;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStatus;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.client.TransferenceClientOption;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.message.ProtocolBufProtocolFactory;
import py.thrift.datanode.service.DataNodeService;
import py.volume.CacheType;
import py.volume.VolumeId;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

public class MockCoordinatorBuilder {

  public static final int DEFAULT_SEGMENT_SIZE = 1024 * 1024; // 1MB
  public static final int DEFAULT_PAGE_SIZE = 8 * 1024; //8KB
  public static final int DEFAULT_MAX_SEGMENT_COUNT = 2047; // 512
  public static final int DEFAULT_SEGMENT_WRAPPED_COUNT = 16;
  public static final int DEFAULT_PAGE_WRAPPED_COUNT = 128;
  public static final int PRIMARY_PORT = 1;
  private static final Logger logger = LoggerFactory.getLogger(MockCoordinatorBuilder.class);
  public static AtomicLong logIdGenerator = new AtomicLong(1);
  private DummyInstanceStore dummyInstanceStore;
  private DummyVolumeInfoRetriever dummyVolumeInfoRetriever;
  private InstanceId instanceId1;
  private InstanceId instanceId2;
  private InstanceId instanceId3;
  private InformationCenterClientFactory informationCenterClientFactory;
  private EndPoint endPoint1;
  private EndPoint endPoint2;
  private EndPoint endPoint3;
  private ByteBuffer byteBuffer1;
  private ByteBuffer byteBuffer2;
  private ByteBuffer byteBuffer3;
  private int segmentCount;
  private VolumeMetadata volumeMetadata;

  private Map<InstanceId, EndPoint> mapInstanceId2EndPoint;
  private Map<EndPoint, ByteBuffer> mapEndPoint2ByteBuffer;

  private MockDataNodeAsyncClientFactory mockDataNodeAsyncClientFactory;
  private MockDataNodeSyncClientFactory mockDataNodeSyncClientFactory;
  private MockNettyClientFactory mockNettyClientFactory;

  private ByteBufAllocator allocator;

  /**
   * xx.
   */
  public MockCoordinatorBuilder(int segmentCount, EndPoint serverEndPoint) {
    if (segmentCount > DEFAULT_MAX_SEGMENT_COUNT) {
      System.out
          .println(
              "please choose little segment count, like less than:[" + DEFAULT_MAX_SEGMENT_COUNT
                  + "]");
      System.exit(-1);
    }

    CoordinatorConfigSingleton.getInstance().setPageSize(DEFAULT_PAGE_SIZE);
    CoordinatorConfigSingleton.getInstance().setSegmentSize(DEFAULT_SEGMENT_SIZE);

    this.segmentCount = segmentCount;
    this.instanceId1 = new InstanceId(RequestIdBuilder.get());
    this.instanceId2 = new InstanceId(RequestIdBuilder.get());
    this.instanceId3 = new InstanceId(RequestIdBuilder.get());

    this.endPoint1 = new EndPoint("127.0.0.1", PRIMARY_PORT);
    this.endPoint2 = new EndPoint("127.0.0.1", PRIMARY_PORT + 1);
    this.endPoint3 = new EndPoint("127.0.0.1", PRIMARY_PORT + 2);

    int volumeSize = DEFAULT_SEGMENT_SIZE * this.segmentCount;
    this.byteBuffer1 = ByteBuffer.allocate(volumeSize);
    this.byteBuffer2 = ByteBuffer.allocate(volumeSize);
    this.byteBuffer3 = ByteBuffer.allocate(volumeSize);

    mapInstanceId2EndPoint = new ConcurrentHashMap<>();
    mapEndPoint2ByteBuffer = new ConcurrentHashMap<>();

    mapInstanceId2EndPoint.put(instanceId1, endPoint1);
    mapInstanceId2EndPoint.put(instanceId2, endPoint2);
    mapInstanceId2EndPoint.put(instanceId3, endPoint3);

    mapEndPoint2ByteBuffer.put(endPoint1, byteBuffer1);
    // mapEndPoint2ByteBuffer.put(endPoint2, byteBuffer2);
    // mapEndPoint2ByteBuffer.put(endPoint3, byteBuffer3);

    this.volumeMetadata = mockVolumeMetadata();

    this.dummyVolumeInfoRetriever = new DummyVolumeInfoRetriever(this.volumeMetadata);

    this.dummyInstanceStore = new DummyInstanceStore();
    this.dummyInstanceStore.setInstances(mockDatanodes(serverEndPoint));
    CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();

    this.mockDataNodeSyncClientFactory = new MockDataNodeSyncClientFactory(
        DataNodeService.Iface.class,
        new TCompactProtocol.Factory(), 0, 0, 0, true);
    this.mockDataNodeSyncClientFactory.withMaxChannelPendingSizeMb(cfg.getMaxChannelPendingSize());
    this.mockDataNodeAsyncClientFactory = new MockDataNodeAsyncClientFactory(
        DataNodeService.AsyncIface.class,
        new TCompactProtocol.Factory(), 0, 0, 0, true);
    this.mockDataNodeAsyncClientFactory.withMaxChannelPendingSizeMb(cfg.getMaxChannelPendingSize());

    TransferenceConfiguration transferenceConfiguration = TransferenceConfiguration
        .defaultConfiguration();

    int poolSize = NumberUtils.toInt(
        System.getProperty("multi.channel.count", String.valueOf(cfg.getConnectionPoolSize())));
    logger.warn("netty channel count:[{}]", poolSize);

    transferenceConfiguration
        .option(TransferenceClientOption.CONNECTION_COUNT_PER_ENDPOINT, poolSize);

    transferenceConfiguration
        .option(TransferenceClientOption.IO_TIMEOUT_MS, cfg.getNettyRequestTimeoutMs());
    transferenceConfiguration
        .option(TransferenceClientOption.IO_CONNECTION_TIMEOUT_MS, cfg.getNettyConnectTimeoutMs());

    allocator = PooledByteBufAllocator.DEFAULT;
    CoordinatorConfigSingleton.getInstance().setSegmentSize(DEFAULT_SEGMENT_SIZE);
    CoordinatorConfigSingleton.getInstance().setPageSize(DEFAULT_PAGE_SIZE);

    try {
      this.mockNettyClientFactory = new MockNettyClientFactory(AsyncDataNode.AsyncIface.class,
          ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
          transferenceConfiguration,
          mapEndPoint2ByteBuffer, allocator, serverEndPoint);
      this.mockNettyClientFactory.init();
    } catch (Exception e) {
      logger.error("can not init MockCoordinatorBuilder", e);
      System.exit(-1);
    }

  }

  public static boolean isPrimary(EndPoint endPoint) {
    return endPoint.getPort() == PRIMARY_PORT;
  }

  public VolumeMetadata getVolumeMetadata() {
    return volumeMetadata;
  }

  /**
   * xx.
   */
  public StorageDriver buildMockCoordinator() {
    CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
    cfg.setEnableLoggerTracer(false);
    Coordinator coordinator = new Coordinator(dummyInstanceStore, mockDataNodeSyncClientFactory,
        mockDataNodeAsyncClientFactory, allocator, mockNettyClientFactory);

    informationCenterClientFactory = new InformationCenterClientFactory();

    coordinator.getVolumeInfoHolderManager().setVolumeInfoRetrieve(dummyVolumeInfoRetriever);
    coordinator.setInformationCenterClientFactory(informationCenterClientFactory);
    return coordinator;
  }

  private Set<Instance> mockDatanodes(EndPoint datanodeEndPoint) {
    Set<Instance> datanodes = new HashSet<>();
    for (Map.Entry<InstanceId, EndPoint> entry : mapInstanceId2EndPoint.entrySet()) {
      Instance datanode = new Instance(entry.getKey(), PyService.DATANODE.name(),
          InstanceStatus.HEALTHY, entry.getValue());

      if (datanodeEndPoint != null && isPrimary(entry.getValue())) {
        datanode.putEndPointByServiceName(PortType.IO, datanodeEndPoint);
      } else {
        datanode.putEndPointByServiceName(PortType.IO, entry.getValue());
      }

      datanodes.add(datanode);
    }
    return datanodes;
  }

  private VolumeMetadata mockVolumeMetadata() {
    long volumeId = RequestIdBuilder.get();
    VolumeMetadata volumeMetadata = new VolumeMetadata(volumeId, volumeId,
        this.segmentCount * DEFAULT_SEGMENT_SIZE,
        DEFAULT_SEGMENT_SIZE, VolumeType.REGULAR, RequestIdBuilder.get(),
        RequestIdBuilder.get());

    volumeMetadata.setSegmentWrappCount(DEFAULT_SEGMENT_WRAPPED_COUNT);
    volumeMetadata.setPageWrappCount(DEFAULT_PAGE_WRAPPED_COUNT);
    Collection<InstanceId> secondaries = new ArrayList<>();
    secondaries.add(instanceId2);
    secondaries.add(instanceId3);
    SegmentMembership segmentMembership = new SegmentMembership(new SegmentVersion(1, 0),
        instanceId1, secondaries);

    for (int i = 0; i < this.segmentCount; i++) {
      SegId segId = new SegId(new VolumeId(volumeId), i);
      SegmentMetadata segmentMetadata = new SegmentMetadata(segId, i);
      volumeMetadata.addSegmentMetadata(segmentMetadata, segmentMembership);
    }

    return volumeMetadata;
  }
}
