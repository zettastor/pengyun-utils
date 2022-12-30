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

import py.common.PyService;

/**
 * xx.
 */
public class ServiceDebuggerFactory {

  /**
   * xx.
   */
  public static AbstractServiceDebugger getServiceDebugger(String serviceName) {
    PyService pyService = null;
    for (PyService tmpService : PyService.values()) {
      if (serviceName.equalsIgnoreCase(tmpService.getServiceName())) {
        pyService = tmpService;
        break;
      }
    }

    if (pyService == null) {
      return null;
    }

    switch (pyService) {
      case INFOCENTER:
        return new InfoCenterAbstractServiceDebugger(pyService);
      case DATANODE:
        return new DataNodeAbstractServiceDebugger(pyService);
      case DIH:
        return new DihAbstractServiceDebugger(pyService);
      case COORDINATOR:
      case DRIVERCONTAINER:
      case DEPLOYMENTDAMON:
      case CONSOLE:
        return new CommonAbstractServiceDebugger(pyService);
      default:
        return null;
    }
  }
}
