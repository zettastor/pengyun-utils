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

package py.deployment.common;

/**
 * A class lists all deployment operation.
 */
public enum DeploymentOperation {
  UPGRADE(1), DEPLOY(2), ACTIVATE(3), DEACTIVATE(4), START(5), RESTART(6), DESTROY(7), WIPEOUT(
      8), STATUS(9), CONFIGURE(
      10), TRANSFER(11), BACKUP_KEY(12), USE_BACKUP_KEY(13);

  private int value;

  private DeploymentOperation(int value) {
    this.value = value;
  }

  /**
   * xx.
   */
  public static DeploymentOperation findValueByName(String name) {
    name = name.toUpperCase();

    if (name.equals(UPGRADE.name())) {
      return UPGRADE;
    }
    if (name.equals(DEPLOY.name())) {
      return DEPLOY;
    }
    if (name.equals(ACTIVATE.name())) {
      return ACTIVATE;
    }
    if (name.equals(START.name())) {
      return START;
    }
    if (name.equals(DEACTIVATE.name())) {
      return DEACTIVATE;
    }
    if (name.equals(RESTART.name())) {
      return RESTART;
    }
    if (name.equals(DESTROY.name())) {
      return DESTROY;
    }
    if (name.equals(WIPEOUT.name())) {
      return WIPEOUT;
    }
    if (name.equals(STATUS.name())) {
      return STATUS;
    }
    if (name.equals(CONFIGURE.name())) {
      return CONFIGURE;
    }
    if (name.equals(TRANSFER.name())) {
      return TRANSFER;
    }
    if (name.equals(BACKUP_KEY.name())) {
      return BACKUP_KEY;
    }
    if (name.equals(USE_BACKUP_KEY.name())) {
      return USE_BACKUP_KEY;
    }

    return null;
  }

  public int getValue() {
    return value;
  }
}
