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

import py.coordinator.worker.ExtendingVolumeUpdater;
import py.drivercontainer.driver.LaunchDriverParameters;
import py.infocenter.client.InformationCenterClientFactory;

public class MockExtendingVolumeUpdater extends ExtendingVolumeUpdater {

  public MockExtendingVolumeUpdater(InformationCenterClientFactory icClientFactory,
      LaunchDriverParameters launchDriverParameters) {
    super(icClientFactory, launchDriverParameters);
  }

  @Override
  public void run() {
    // do nothing
  }
}
