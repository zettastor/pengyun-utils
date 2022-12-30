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
