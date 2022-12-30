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