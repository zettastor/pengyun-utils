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

package py.nettysetup;

import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.datanode.PyReadResponse;

public class SimpleReadCallback<T> extends AbstractMethodCallback<T> {

  private static final Logger logger = LoggerFactory.getLogger(SimpleWriteCallback.class);
  private Semaphore semaphore;
  private Long requestId;
  private PyReadResponse pyReadResponse;

  /**
   * xx.
   */
  public SimpleReadCallback(Long requestId, Semaphore semaphore) {
    this.requestId = requestId;
    this.semaphore = semaphore;
  }

  @Override
  public void complete(T object) {
    logger.debug("%%%%write call back:<<<<{}>>>> successfully", this.requestId);

    // should release read data from channel
    pyReadResponse = (PyReadResponse) object;
    pyReadResponse.getData().release();
    endProcess();
  }

  @Override
  public void fail(Exception e) {
    logger.error("!!!!write call back:<<<<{}>>>> caught an exception", this.requestId, e);
    endProcess();
  }

  private void endProcess() {
    if (this.semaphore != null) {
      this.semaphore.release();
    }
  }
}
