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
import py.debug.DebuggerOutputImpl;
import py.debug.DynamicParamConfig;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class ShowLogLevel extends ShowConfigCmd {

  public ShowLogLevel(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  protected void print(String[] args, Map<String, String> configurations) {
    for (Map.Entry<String, String> value : configurations.entrySet()) {
      if (value.getKey().contains(DynamicParamConfig.LOG_LEVEL)) {
        DebuggerOutputImpl.getInstance().print(value.getKey() + "=" + value.getValue() + "\n");
      }
    }
  }
}
