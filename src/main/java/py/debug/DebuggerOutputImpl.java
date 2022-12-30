/*
 * Copyright© 2015 Pengyun Network Inc. All rights reserved.
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

/**
 * xx.
 */
public class DebuggerOutputImpl implements DebuggerOutput {

  private static DebuggerOutputImpl instance = null;
  String copyRight =
      "This tool is for debugging the storage system "
          + "of Pengyun Network. You should know what you are doing.\n"
          + "It was written by David Wang. You can contact him with any questions.\n"
          + "Copyright© 2015 Pengyun Network Inc. All rights reserved. \n";
  String usage = "useage: java -jar configurator.jar ipAdr service [port]\n"
      + "Please specify ip address, serivce and port.\n"
      + "ip address: The server you want to debug (mandatory).\n"
      + "Serivce: InfoCenter, ControlCenter, DataNode, "
      + "DIH, DriverContainer, Coordinator, DeploymentDaemon,"
      + " MonitorServer, SystemDaemon, FSServer (mandatory).\n"
      + "Port: On which the serivce is listenning(optinoal).\n";
  private String prompt = null;

  private DebuggerOutputImpl() {
  }

  /**
   * xx.
   */
  public static DebuggerOutputImpl getInstance() {
    if (instance == null) {
      synchronized (DebuggerOutputImpl.class) {
        if (instance == null) {
          instance = new DebuggerOutputImpl();
        }
      }
    }
    return instance;
  }

  public String getPrompt() {
    return prompt;
  }

  public void setPrompt(String prompt) {
    this.prompt = prompt;
  }

  @Override
  public void print(String info) {
    System.out.print(info);
  }

}
