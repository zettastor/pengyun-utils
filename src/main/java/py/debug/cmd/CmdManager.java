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
