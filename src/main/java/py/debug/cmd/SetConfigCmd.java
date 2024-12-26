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

import java.util.HashMap;
import java.util.Map;
import py.common.RequestIdBuilder;
import py.debug.DebuggerOutputImpl;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;

/**
 * xx.
 */
public class SetConfigCmd extends AbstractCmd {

  public SetConfigCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    SetConfigurationsRequest setRequest = new SetConfigurationsRequest();
    GetConfigurationsRequest viewRequest = new GetConfigurationsRequest();
    GetConfigurationsResponse response;

    if (args == null || args.length != 2) {
      usage();
      return;
    }

    String key = args[0];
    String value = args[1];
    // check the key if exist
    try {
      viewRequest.setRequestId(RequestIdBuilder.get());
      response = debugConfigurator.getConfigurations(viewRequest);
    } catch (Exception ex) {
      ex.printStackTrace();
      return;
    }

    boolean foundKey = false;
    Map<String, String> keyAndValue = new HashMap<String, String>();
    Map<String, String> configurations = response.getResults();
    for (String mapKey : configurations.keySet()) {
      if (mapKey.equalsIgnoreCase(key)) {
        keyAndValue.put(mapKey, value);
        foundKey = true;
        break;
      }
    }

    if (!foundKey) {
      DebuggerOutputImpl.getInstance()
          .print("key " + key + " does not exist in service configuration");
      return;
    }

    try {
      setRequest.setConfigurations(keyAndValue);
      setRequest.setRequestId(RequestIdBuilder.get());
      debugConfigurator.setConfigurations(setRequest);
    } catch (Exception ex) {
      ex.printStackTrace();
      return;
    }

    DebuggerOutputImpl.getInstance().print("Set " + key + " with value " + value + " successfully");

  }

  @Override
  public void usage() {
    StringBuffer sb = new StringBuffer();
    sb.append("setConfig key value\n");
    DebuggerOutputImpl.getInstance().print(sb.toString());
  }

}