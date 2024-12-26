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
