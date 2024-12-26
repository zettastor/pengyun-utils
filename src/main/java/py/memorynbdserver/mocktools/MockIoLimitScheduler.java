

package py.memorynbdserver.mocktools;

import py.icshare.qos.IoLimitScheduler;
import py.io.qos.IoLimitManager;

public class MockIoLimitScheduler extends IoLimitScheduler {

  public MockIoLimitScheduler(IoLimitManager ioLimitManager) {
    super(ioLimitManager);
  }

  @Override
  public void open() {

  }


}
