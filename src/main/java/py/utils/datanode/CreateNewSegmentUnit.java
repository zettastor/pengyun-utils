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
