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
