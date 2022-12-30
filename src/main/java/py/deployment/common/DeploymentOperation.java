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
