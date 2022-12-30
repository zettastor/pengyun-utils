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

package py.debug.service;

import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.common.PyService;
import py.debug.cmd.Cmd;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class CommonAbstractServiceDebugger extends AbstractServiceDebugger {

  public CommonAbstractServiceDebugger(PyService pyService) {
    super(pyService);
  }

  @Override
  protected void addCmd(DebugConfigurator.Iface debugConfigurator,
      List<ImmutablePair<String, Cmd>> cmdList) {
    super.addCmd(debugConfigurator, cmdList);
  }
}
