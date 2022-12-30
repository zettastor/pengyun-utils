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

import py.thrift.datanode.service.DataNodeService;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.WriteMutationLogsDisableRequest;

/**
 * this class is used for datanode.
 *
 */
public class SwitchProcessWriteMutationLogEnableOrDisableCmd extends AbstractCmd {

  public SwitchProcessWriteMutationLogEnableOrDisableCmd(
      DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    if (args == null || args.length != 2) {
      usage();
      return;
    }

    long volumeId = Long.valueOf(args[0]);
    int segIndex = Integer.valueOf(args[1]);
    WriteMutationLogsDisableRequest request = new WriteMutationLogsDisableRequest(1, volumeId,
        segIndex);
    try {
      ((DataNodeService.Iface) debugConfigurator).writeMutationLogDisable(request);
    } catch (Exception ex) {
      ex.printStackTrace();
      return;
    }

  }

  @Override
  public void usage() {
    StringBuffer sb = new StringBuffer();
    sb.append("switchEnableWriteMutations volumeId segId\n");
    out.print(sb.toString());
  }
}
