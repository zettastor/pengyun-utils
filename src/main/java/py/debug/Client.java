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

package py.debug;

import java.util.Arrays;
import java.util.Scanner;
import py.debug.cmd.CmdManager;
import py.debug.service.AbstractServiceDebugger;
import py.debug.service.ServiceDebuggerFactory;

/**
 * xx.
 */
public class Client {

  private static CmdManager cmdManager = CmdManager.getInstance();
  private static DebuggerOutputImpl out = DebuggerOutputImpl.getInstance();

  /**
   * xx.
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      out.print(out.usage);
      System.exit(1);
    }

    String host = args[0].trim();
    String serviceName = args[1].trim();
    String port = null;
    if (args.length > 2) {
      port = args[2].trim();
    }

    final Client client = new Client();

    AbstractServiceDebugger abstractServiceDebugger = ServiceDebuggerFactory
        .getServiceDebugger(serviceName);
    if (abstractServiceDebugger == null) {
      out.print(out.usage);
      System.exit(1);
    }

    abstractServiceDebugger.setHostName(host);
    abstractServiceDebugger.setPort(port);
    if (!abstractServiceDebugger.init()) {
      String err = "Cannot connect host " + host + " with service " + serviceName;
      if (port != null) {
        err = err + " on port " + port;
      }
      out.print(err);
      System.exit(0);
    }

    cmdManager.setServiceType(abstractServiceDebugger.getServiceType());
    out.print(out.copyRight);
    out.setPrompt(abstractServiceDebugger.getPrefix());
    out.print(out.getPrompt());
    client.debug();
  }

  /**
   * xx.
   */
  public void debug() {
    Scanner sc = new Scanner(System.in);
    while (!cmdManager.isExitDebug()) {
      String cmdString = sc.nextLine();

      String[] str = cmdString.split(" ");
      for (int idx = 0; idx < str.length; idx++) {
        str[idx] = str[idx].trim();
      }
      String cmd = str[0];
      String[] cmdArgs = Arrays.copyOfRange(str, 1, str.length);
      try {
        cmdManager.doCmd(cmd, cmdArgs);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    sc.close();
  }
}
