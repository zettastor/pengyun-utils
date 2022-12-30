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

package py.memorynbdserver.mocktools;

import py.io.qos.IoLimitManager;
import py.io.qos.IoLimitationEntry;
import py.periodic.UnableToStartException;

public class MockIoLimitManager implements IoLimitManager {

  @Override
  public void updateLimitationsAndOpen(IoLimitationEntry ioLimitation)
      throws UnableToStartException {

  }

  @Override
  public void close() {

  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public IoLimitationEntry getIoLimitationEntry() {
    return null;
  }

  @Override
  public void tryGettingAnIo() {

  }

  @Override
  public void tryThroughput(long size) {

  }

  @Override
  public void slowDownExceptFor(long volumeId, int level) {

  }

  @Override
  public void resetSlowLevel(long volumeId) {

  }
}
