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

package py.utils.gitlab;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * xx.
 */
public class Launcher {

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      showUsage();
      return;
    }

    String token = args[0];

    String commandString = args[1];
    BranchBlocker.Command command;
    try {
      command = BranchBlocker.Command.get(commandString);
    } catch (Exception e) {
      showUsage();
      return;
    }

    String projectIdsStr = args[2];
    String regEx = ",";

    Pattern pattern = Pattern.compile(regEx);
    String[] projectIds = pattern.split(projectIdsStr);

    BranchBlocker branchBlocker = new BranchBlocker();
    branchBlocker.doCmd(token, command, new ArrayList<String>(Arrays.asList(projectIds)));
  }

  private static void showUsage() {
    StringBuffer sb = new StringBuffer();
    sb.append("\n\tYou can input command like this : \n");
    sb.append("\tjava -jar blocker.jar [token] [operator] [project ids]\n");
    sb.append("\t[operator] can be chosen in: ");

    String cmds = String.format("%s\n", BranchBlocker.Command.list().toString());
    sb.append(cmds);
    sb.append("\n");

    sb.append("\t[project ids] is format as: '1,2,9,32'");
    sb.append("\tFor example: java -jar blocker.jar <your token> list all");
    sb.append("\n");
    sb.append("\tFor any questions, please ask sxl for help.\n");
    System.out.println(sb.toString());
  }
}
