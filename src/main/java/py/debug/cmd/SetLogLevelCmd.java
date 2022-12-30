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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Level;
import org.apache.thrift.TException;
import py.debug.DebuggerOutputImpl;
import py.debug.DynamicParamConfig;
import py.thrift.share.DebugConfigurator;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.SetConfigurationsRequest;

/**
 * xx.
 */
public class SetLogLevelCmd extends AbstractCmd {

  public SetLogLevelCmd(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  public void doCmd(String[] args) {
    if (args == null || args.length == 0) {
      usage();
      return;
    }

    String logLevel;
    String className = null;

    if (args.length == 2) {
      className = args[0];
      logLevel = args[1].toUpperCase();
    } else {
      logLevel = args[0].toUpperCase();
    }

    String[] logLevels = {Level.ALL.toString(), Level.TRACE.toString(), Level.DEBUG.toString(),
        Level.INFO.toString(), Level.WARN.toString(), Level.ERROR.toString(),
        Level.FATAL.toString(),
        Level.OFF.toString()};

    if (!Arrays.asList(logLevels).contains(logLevel)) {
      usage();
      return;
    }

    String value = logLevel;
    if (className != null) {
      value = value + DynamicParamConfig.SEPARATOR + className;
    }

    SetConfigurationsRequest request = new SetConfigurationsRequest();
    Map<String, String> map = new HashMap<>();
    map.put(DynamicParamConfig.LOG_LEVEL, value);
    request.setConfigurations(map);

    try {
      debugConfigurator.setConfigurations(request);
    } catch (TException e) {
      DebuggerOutputImpl.getInstance().print("can not setLogLevel: " + e.toString());
      return;
    }

    if (className != null) {
      GetConfigurationsRequest getRequest = new GetConfigurationsRequest();
      GetConfigurationsResponse configurations = null;
      try {
        configurations = debugConfigurator.getConfigurations(getRequest);
      } catch (TException e) {
        e.printStackTrace();
      }
      Map<String, String> results = configurations.getResults();
      for (Map.Entry<String, String> entry : results.entrySet()) {
        if (entry.getKey().contains(className)) {
          DebuggerOutputImpl.getInstance().print("set successfully!");
          return;
        }
      }
      DebuggerOutputImpl.getInstance().print("set failed!");
      return;
    }

    DebuggerOutputImpl.getInstance().print("set successfully!");
  }

  @Override
  public void usage() {
    String usageInfo =
        "setLogLevel [className] logLevel\n"
            + "logLevel: ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, OFF\n"
            + "[className] is only valid for internal version";
    DebuggerOutputImpl.getInstance().print(usageInfo);
  }

}
