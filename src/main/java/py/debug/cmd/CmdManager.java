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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.common.PyService;
import py.debug.DebuggerOutputImpl;

/**
 * xx.
 */
public class CmdManager {

  private static CmdManager instance = null;
  private HashMap<PyService, List<ImmutablePair<?, ?>>> cmdMap =
      new HashMap<PyService, List<ImmutablePair<?, ?>>>();
  private PyService serviceType;
  private boolean exitDebug = false;
  private DebuggerOutputImpl out = DebuggerOutputImpl.getInstance();

  private CmdManager() {
  }

  /**
   * xx.
   */
  public static CmdManager getInstance() {
    if (instance == null) {
      synchronized (CmdManager.class) {
        if (instance == null) {
          instance = new CmdManager();
        }
      }
    }
    return instance;
  }

  /**
   * Register cmd.
   */
  public void registeCmd(PyService type, List<ImmutablePair<String, Cmd>> cmdArray) {
    List<ImmutablePair<?, ?>> cmds = cmdMap.get(type);
    if (cmds == null) {
      cmds = new ArrayList<ImmutablePair<?, ?>>();
      cmdMap.put(type, cmds);
    }
    for (ImmutablePair<?, ?> acmdarray : cmdArray) {
      if (!cmds.contains(acmdarray)) {
        cmds.add(acmdarray);
      }
    }
  }

  /**
   * do cmd according to String.
   */
  public void doCmd(String cmdName, String[] args) {
    boolean found = false;
    List<ImmutablePair<?, ?>> cmdList = cmdMap.get(serviceType);
    for (ImmutablePair<?, ?> cmdInList : cmdList) {
      String cmdStr = (String) cmdInList.getLeft();
      if (cmdStr.equalsIgnoreCase(cmdName)) {
        found = true;
        Cmd cmd = (Cmd) cmdInList.getRight();
        cmd.doCmd(args);
        if (!exitDebug) {
          out.print("\n" + out.getPrompt());
        }
      }
    }
    if (!found) {
      out.print(cmdName + " is not found");
      out.print("\n" + out.getPrompt());
    }
  }

  public List<ImmutablePair<?, ?>> getCmds() {
    return cmdMap.get(serviceType);
  }

  public PyService getServiceType() {
    return serviceType;
  }

  public void setServiceType(PyService serviceType) {
    this.serviceType = serviceType;
  }

  public boolean isExitDebug() {
    return exitDebug;
  }

  public void setExitDebug(boolean exitDebug) {
    this.exitDebug = exitDebug;
  }

}
