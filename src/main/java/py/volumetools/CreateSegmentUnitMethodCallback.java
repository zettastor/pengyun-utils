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

package py.volumetools;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.thrift.datanode.service.DataNodeService.AsyncClient.createSegmentUnit_call;
import py.thrift.share.SegmentExistingExceptionThrift;

class CreateSegmentUnitMethodCallback implements
    AsyncMethodCallback<createSegmentUnit_call> {

  private static final Logger logger = LoggerFactory
      .getLogger(CreateSegmentUnitMethodCallback.class);

  private volatile CountDownLatch latch;
  private AtomicInteger numGoodResponses;

  public CreateSegmentUnitMethodCallback(CountDownLatch latch, AtomicInteger numGoodResponses) {
    this.latch = latch;
    this.numGoodResponses = numGoodResponses;
  }

  @Override
  public void onComplete(createSegmentUnit_call createSegmentUnitCall) {
    try {
      createSegmentUnitCall.getResult();
      numGoodResponses.incrementAndGet();
    } catch (SegmentExistingExceptionThrift e) {
      numGoodResponses.incrementAndGet();
      logger.error("the segment unit that we want to create already exists", e);
    } catch (Throwable e) {
      logger.error("onComplete, find some error:", e);
    }

    latch.countDown();
  }

  public void onError(Exception e) {
    logger.error("onError, find some error:", e);
    latch.countDown();
  }
}