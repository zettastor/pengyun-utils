

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
