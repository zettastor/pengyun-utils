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

package py.debug.cmd;

import static py.common.Constants.SUPERADMIN_ACCOUNT_ID;

import java.util.List;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.thrift.TException;
import py.RequestResponseHelper;
import py.archive.segment.SegmentMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.icshare.ListVolumesRequest;
import py.thrift.icshare.ListVolumesResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.AccessDeniedExceptionThrift;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.ServiceIsNotAvailableThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeNotFoundExceptionThrift;
import py.volume.VolumeMetadata;

/**
 * xx.
 */
public class GetEpochCmd extends AbstractCmd {

  private static final JexlEngine jexl = new JexlEngine();
  private String expressionString = null;
  private String[] fieldName = {"volumeId", "segIndex", "epoch", "generation"};

  public GetEpochCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    if (args == null || args.length != 1) {
      usage();
      return;
    }

    expressionString = args[0];
    if (expressionString == null) {
      out.print("Please input your expression\n");
      return;
    }

    StringBuffer sb = new StringBuffer();
    sb.append("Get epoch result:\n\n");
    sb.append("+---------------------+--------------+-----------+---------------+\n");
    sb.append("|      volumeId       |   segIndex   |   epoch   |   generation  |\n");
    sb.append("+---------------------+--------------+-----------+---------------+\n");
    ListVolumesRequest request = new ListVolumesRequest();
    request.setAccountId(SUPERADMIN_ACCOUNT_ID);
    request.setRequestId(RequestIdBuilder.get());
    ListVolumesResponse response = null;
    try {
      response = ((InformationCenter.Iface) debugConfigurator).listVolumes(request);
    } catch (TException e) {
      e.printStackTrace();
    }

    for (VolumeMetadataThrift volumeThrift : response.getVolumes()) {
      boolean flag = false;
      // List<SegmentMetadataThrift> segments = volumeThrift.getSegmentsMetadata();

      GetVolumeRequest getVolumeRequest = new GetVolumeRequest();
      getVolumeRequest.setRequestId(RequestIdBuilder.get());
      getVolumeRequest.setAccountId(SUPERADMIN_ACCOUNT_ID);
      getVolumeRequest.setVolumeId(volumeThrift.getVolumeId());
      GetVolumeResponse getVolumeResponse;

      try {
        getVolumeResponse = ((InformationCenter.Iface) debugConfigurator)
            .getVolume(getVolumeRequest);
      } catch (AccessDeniedExceptionThrift e) {
        out.print("Access denied");
        return;
      } catch (InternalErrorThrift e) {
        out.print("Internal error of infocenter");
        return;
      } catch (InvalidInputExceptionThrift e) {
        out.print("Invalid input");
        return;
      } catch (VolumeNotFoundExceptionThrift e) {
        out.print("Volume not found in infocenter");
        return;
      } catch (ServiceHavingBeenShutdownThrift e) {
        out.print("Service has been shutdown");
        return;
      } catch (ServiceIsNotAvailableThrift e) {
        out.print("service is not aviailable");
        return;
      } catch (TException e) {
        out.print("Unknown expression");
        return;
      }

      if (!getVolumeResponse.isSetVolumeMetadata()) {
        continue;
      }

      VolumeMetadata volumeMetadata = RequestResponseHelper
          .buildVolumeFrom(getVolumeResponse.getVolumeMetadata());
      for (SegmentMetadata segment : volumeMetadata.getSegmentTable().values()) {
        List<SegmentUnitMetadata> segmentUnits = segment.getSegmentUnits();

        for (SegmentUnitMetadata segmentUnit : segmentUnits) {
          try {
            if (selected(segmentUnit)) {
              sb.append(String
                  .format("  %d        %d        \n", segmentUnit.getSegId().getVolumeId().getId(),
                      segmentUnit.getSegId().getIndex()));
              flag = true;
            }
          } catch (Exception e) {
            out.print("Illegal expression");
            return;
          }
        }
      }

      if (flag) {
        sb.append("^----------------------------------------------------------------^\n");
      }
    }

    out.print(sb.toString());
  }

  @Override
  public void usage() {
    StringBuffer sb = new StringBuffer();
    sb.append("\n\tYou can input your filter expression like this : \n");
    sb.append("\t[field name] [operator] [value]\n");
    sb.append("\t[field name] can only be chosen in (volumeId segIndex epoch generation)\n");
    sb.append(
        "\t[operator] can be chosen in ('>','>=','==','<','<=','!=') "
            + "and any other operation that is supported by JEXL\n");
    sb.append("\t[value] is the right value of your expression\n\n");
    sb.append("\tAlso, you can combine one single expression with the others just like this:\n");
    sb.append("\t[expression1] [expression connector] [expression2]\n");
    sb.append(
        "\t[expression connector] can be chosen in ('&&','||','!','&','|','^','~' and so on)\n");
    sb.append("\n");
    sb.append(
        "\tFor example: getEpoch volumeId=="
            + "1996383492428932748&&segIndex==0&&epoch>=2&&generation>=1");
    sb.append("\n");
    sb.append("\n\tAny JEXL syntax questions, please take a look at the offical sit of JEXL:\n");
    sb.append("\thttps://commons.apache.org/proper/commons-jexl/reference/syntax.html#Operators\n");
    sb.append("\tOr,Ask sxl for help.\n");
    out.print(sb.toString());
  }

  private boolean selected(SegmentUnitMetadata segUnit) throws Exception {
    try {
      Expression expression = jexl.createExpression(expressionString);
      JexlContext jc = new MapContext();

      jc.set("volumeId", segUnit.getSegId().getVolumeId().getId());
      jc.set("segIndex", segUnit.getSegId().getIndex());
      return (boolean) expression.evaluate(jc);
    } catch (Exception e) {
      out.print("Caught an exception" + e.toString());
      throw new Exception();
    }
  }
}
