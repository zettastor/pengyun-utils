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
