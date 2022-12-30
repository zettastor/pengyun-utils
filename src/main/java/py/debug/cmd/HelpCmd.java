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
