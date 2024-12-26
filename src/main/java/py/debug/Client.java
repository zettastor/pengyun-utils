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
