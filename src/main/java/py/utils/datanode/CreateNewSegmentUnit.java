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

package py.utils.datanode;

import java.util.Arrays;
import java.util.List;
import org.apache.thrift.TException;
import py.RequestResponseHelper;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.exception.GenericThriftClientFactoryException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.NotSupportedExceptionThrift;
import py.volume.CacheType;
import py.volume.VolumeType;

/**
 * xx.
 */
public class CreateNewSegmentUnit {

  public static int dataNodePort = 10011;

  /**
   * xx.
   */
  public static void main(String[] args) {
    GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory =
        GenericThriftClientFactory
        .create(DataNodeService.Iface.class).withMaxChannelPendingSizeMb(200);
    List<String> ips = IpUtils.getIps(args[0]);

    boolean result = true;
    for (String ip : ips) {
      EndPoint endPoint = EndPointParser.parseLocalEndPoint(dataNodePort, ip);
      try {
        DataNodeService.Iface client = dataNodeSyncClientFactory
            .generateSyncClient(endPoint, 15000, 10000);
        SegmentMembership membership = new SegmentMembership(new SegmentVersion(1, 7),
            new InstanceId(1993250213217430261L),
            Arrays.asList(new InstanceId(2727357334438952084L)), null,
            Arrays.asList(new InstanceId(5686102141305285004L)), null);

        CreateSegmentUnitRequest request = RequestResponseHelper.buildCreateSegmentUnitRequest(
            new SegId(959382706027878069L, 57), membership, 4, VolumeType.REGULAR,
             1168129619147965638L, SegmentUnitType.Normal);
        client.createSegmentUnit(request);
        System.out.println("for the ip " + ip + " , operation success!");
      } catch (GenericThriftClientFactoryException e) {
        e.printStackTrace();
        System.out.println("fail to connect to datanode, wait 5 seconds to try again");
        try {
          Thread.sleep(5000);
        } catch (Exception ex) {
          ex.printStackTrace();
        }
        result = false;
      } catch (NotSupportedExceptionThrift e) {
        e.printStackTrace();
        result = false;
      } catch (TException e) {
        System.out.println("Check magic number?" + e);
        result = false;
      }
    }
    if (!result) {
      System.out.println("failed to clean log for ips : " + ips);
      System.exit(-1);
    }
  }
}
