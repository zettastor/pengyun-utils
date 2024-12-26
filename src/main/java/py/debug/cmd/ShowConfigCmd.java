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

import java.util.Map;
import py.common.RequestIdBuilder;
import py.debug.DebuggerOutputImpl;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;

/**
 * xx.
 */
public class ShowConfigCmd extends AbstractCmd {

  public ShowConfigCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    GetConfigurationsRequest request = new GetConfigurationsRequest();
    GetConfigurationsResponse response = null;
    try {
      request.setRequestId(RequestIdBuilder.get());
      response = debugConfigurator.getConfigurations(request);
    } catch (Exception ex) {
      DebuggerOutputImpl.getInstance().print("can not get configuration " + ex.toString());
    }

    Map<String, String> configurations = response.getResults();
    print(args, configurations);
  }

  protected void print(String[] args, Map<String, String> configurations) {
    for (Map.Entry<String, String> value : configurations.entrySet()) {
      if (args.length == 0 || value.getKey().equalsIgnoreCase(args[0])) {
        DebuggerOutputImpl.getInstance().print(value.getKey() + "=" + value.getValue() + "\n");
      }
    }
  }

  @Override
  public void usage() {

  }

}
