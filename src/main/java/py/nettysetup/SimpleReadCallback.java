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
