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

import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.debug.DebuggerOutput;
import py.debug.DebuggerOutputImpl;

/**
 * xx.
 */
public class HelpCmd implements Cmd {

  private DebuggerOutput out = null;

  public HelpCmd() {
    out = DebuggerOutputImpl.getInstance();
  }

  @Override
  public void doCmd(String[] args) {
    StringBuffer sb = new StringBuffer();
    List<ImmutablePair<?, ?>> cmdList = CmdManager.getInstance().getCmds();
    for (ImmutablePair<?, ?> cmd : cmdList) {
      sb.append(cmd.getLeft() + "\n");
    }
    out.print(sb.toString());
  }

  @Override
  public void usage() {

  }
}
