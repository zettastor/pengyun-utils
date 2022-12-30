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

package py.utils.performance.test;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.thrift.testing.service.PerformanceTestRequestThrift;
import py.thrift.testing.service.PerformanceTestResponseThrift;
import py.thrift.testing.service.PerformanceTestService;
import py.thrift.testing.service.PerformanceTestService.AsyncClient.testPingPang_call;
import py.thrift.testing.service.PerformanceTestService.AsyncClient.testRead_call;
import py.thrift.testing.service.PerformanceTestService.AsyncClient.testWrite_call;

/**
 * xx.
 */
public class PerformanceTestClient {

  private static final Logger logger = LoggerFactory.getLogger(PerformanceTestClient.class);
  private static final String HELP_CONTENT =
      "--synchronize <false> --readSize <4096> --writeSize <4096> --workerThreadCount <1> "
          + "--serverAddress <IP:33333> --workPoolCount <1> "
          + "--responseThreadCount <twice of cpu> --testType<pingpang|read|write>";
  private static GenericThriftClientFactory<PerformanceTestService.AsyncIface> asyncFactory = null;
  private static GenericThriftClientFactory<PerformanceTestService.Iface> syncFactory = null;

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {

    final CommondLineArgs cla = new CommondLineArgs();
    final int type;
    try {
      @SuppressWarnings("unused")
      JCommander commander = new JCommander(cla, args);
      type = getType(cla.testType);
    } catch (Exception e) {
      System.out.println(HELP_CONTENT);
      return;
    }

    if (cla.synchronize) {
      syncFactory = GenericThriftClientFactory.create(PerformanceTestService.Iface.class);
    } else {
      asyncFactory = GenericThriftClientFactory.create(PerformanceTestService.AsyncIface.class);
    }

    final EndPoint endPoint = EndPointParser
        .parseLocalEndPoint(cla.serverAddress, InetAddress.getLocalHost().getHostAddress());
    final CountDownLatch mainLatch = new CountDownLatch(cla.workerThreadCount);

    System.out
        .println("synchronize: " + cla.synchronize + ", readSize: " + cla.readSize + ", writeSize: "
            + cla.writeSize + ", workerThreadCount: " + cla.workerThreadCount + ", serverAddress: "
            + endPoint + ", testType: " + cla.testType + ", responseThreadCount: "
            + cla.responseThreadCount + ", workPoolCount: " + cla.workPoolCount);

    for (int i = 0; i < cla.workerThreadCount; i++) {
      Thread thread = new Thread() {
        public void run() {
          try {
            if (cla.synchronize) {
              PerformanceTestService.Iface client = syncFactory.generateSyncClient(endPoint, 5000);
              if (type == 0) {
                testSyncPingPang(client);
              } else if (type == 1) {
                testSyncRead(client, cla.readSize);
              } else if (type == 2) {
                testSyncWrite(client, cla.writeSize);
              } else {
                throw new Exception("synchronize type value: " + type);
              }
            } else {
              PerformanceTestService.AsyncIface client = asyncFactory
                  .generateAsyncClient(endPoint, 5000);
              if (type == 0) {
                testAsyncPingPang(client);
              } else if (type == 1) {
                testAsyncRead(client, cla.readSize);
              } else if (type == 2) {
                testAsyncWrite(client, cla.writeSize);
              } else {
                throw new Exception("asynchronize type value: " + type);
              }
            }

          } catch (Exception e) {
            logger.error("caugth an exception", e);
          } finally {
            mainLatch.countDown();
          }
        }
      };
      thread.start();
    }

    mainLatch.await();

    if (asyncFactory != null) {
      asyncFactory.close();
    }

    if (syncFactory != null) {
      syncFactory.close();
    }
  }

  static int getType(String type) throws Exception {
    String[] types = {"pingpang", "read", "write"};
    for (int i = 0; i < types.length; i++) {
      if (types[i].equals(type)) {
        return i;
      }
    }

    throw new Exception("there is no this type: " + type);
  }

  static void testSyncPingPang(PerformanceTestService.Iface client) {
    long id = 0;
    while (true) {
      try {
        PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putLong(0, request.getRequestId());
        long time = System.currentTimeMillis();
        byteBuffer.putLong(8, time);
        request.setData(byteBuffer.array());
        PerformanceTestResponseThrift response = null;
        try {
          response = client.testPingPang(request);
        } catch (TTransportException e) {
          if (e.getType() == TTransportException.TIMED_OUT) {
            logger.info("caught an exception", e);
            continue;
          }
        }

        if (response.getData().length != request.getData().length) {
          logger.warn("length mismatch, old: {}, new: {}", request.getData().length,
              response.getData().length);
        }
        ByteBuffer temp = ByteBuffer.wrap(response.getData());
        long newId = temp.getLong();
        long newTime = temp.getLong();

        if (newId != request.getRequestId() || newTime != time) {
          logger.warn(" id: {}, new id: {}, time: {}, new time: {}", request.getRequestId(), newId,
              time,
              newTime);
        }

      } catch (Exception e) {
        logger.info("caught an exception", e);
        return;
      }
    }
  }

  static void testSyncRead(PerformanceTestService.Iface client, int readSize) {
    long id = 0;
    while (true) {
      try {
        PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(0, readSize);
        request.setData(byteBuffer.array());
        PerformanceTestResponseThrift response = client.testRead(request);

        Validate.isTrue(response.getData().length == readSize);
      } catch (Exception e) {
        logger.info("caught an exception", e);
        return;
      }
    }
  }

  static void testSyncWrite(PerformanceTestService.Iface client, int writeSize) {
    long id = 0;
    while (true) {
      try {
        PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(writeSize);
        byteBuffer.putInt(0, writeSize);
        request.setData(byteBuffer.array());
        PerformanceTestResponseThrift response = client.testWrite(request);

        Validate.isTrue(response.getData().length == writeSize);
      } catch (Exception e) {
        logger.info("caught an exception", e);
        return;
      }
    }
  }

  static void testAsyncPingPang(PerformanceTestService.AsyncIface client) {
    final AtomicBoolean error = new AtomicBoolean(false);
    long id = 0;
    while (true) {
      try {
        final PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(16);
        byteBuffer.putLong(0, request.getRequestId());
        final long time = System.currentTimeMillis();
        byteBuffer.putLong(8, time);
        request.setData(byteBuffer.array());

        final CountDownLatch latch = new CountDownLatch(1);
        AsyncMethodCallback<testPingPang_call> callback =
            new AsyncMethodCallback<testPingPang_call>() {
              @Override
              public void onError(Exception e) {
                logger.error("caught an exception", e);
                error.set(true);
                if (latch != null) {
                  latch.countDown();
                }
              }

              @Override
              public void onComplete(testPingPang_call re) {
                try {
                  PerformanceTestResponseThrift response = re.getResult();

                  if (response.getData().length != request.getData().length) {
                    logger.warn("length mismatch, old: {}, new: {}", request.getData().length,
                        response.getData().length);
                  }
                  ByteBuffer temp = ByteBuffer.wrap(response.getData());
                  long newId = temp.getLong();
                  long newTime = temp.getLong();

                  if (newId != request.getRequestId() || newTime != time) {
                    logger
                        .warn("new id: {}, old id: {}, time: {}, new time: {}",
                            request.getRequestId(),
                            newId, time, newTime);
                  }
                } catch (Exception e) {
                  logger.error("caught an exception", e);
                  error.set(true);
                } finally {
                  if (latch != null) {
                    latch.countDown();
                  }
                }
              }
            };

        client.testPingPang(request, callback);
        latch.await();
        if (error.get()) {
          logger.error("meet error");
          break;
        }
      } catch (Exception e) {
        logger.info("caught an exception", e);
        return;
      }
    }
  }

  static void testAsyncRead(PerformanceTestService.AsyncIface client, final int readSize) {
    final AtomicBoolean error = new AtomicBoolean(false);
    long id = 0;
    while (true) {
      try {
        PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(0, readSize);
        request.setData(byteBuffer.array());

        final CountDownLatch latch = new CountDownLatch(1);
        AsyncMethodCallback<testRead_call> callback = new AsyncMethodCallback<testRead_call>() {
          @Override
          public void onError(Exception e) {
            logger.error("caught an exception", e);
            error.set(true);
            if (latch != null) {
              latch.countDown();
            }
          }

          @Override
          public void onComplete(testRead_call re) {
            try {
              PerformanceTestResponseThrift response = re.getResult();
              Validate.isTrue(response.getData().length == readSize);
            } catch (Exception e) {
              logger.error("caught an exception", e);
              error.set(true);
            } finally {
              if (latch != null) {
                latch.countDown();
              }
            }
          }
        };

        client.testRead(request, callback);
        latch.await();
        if (error.get()) {
          logger.error("meet error");
          break;
        }
      } catch (Exception e) {
        logger.error("", e);
      }
    }

  }

  static void testAsyncWrite(PerformanceTestService.AsyncIface client, final int writeSize) {
    final AtomicBoolean error = new AtomicBoolean(false);
    long id = 0;
    while (true) {
      try {
        PerformanceTestRequestThrift request = new PerformanceTestRequestThrift();
        request.setRequestId(id++);

        ByteBuffer byteBuffer = ByteBuffer.allocate(writeSize);
        byteBuffer.putInt(0, writeSize);
        request.setData(byteBuffer.array());

        final CountDownLatch latch = new CountDownLatch(1);
        AsyncMethodCallback<testWrite_call> callback = new AsyncMethodCallback<testWrite_call>() {
          @Override
          public void onError(Exception e) {
            logger.error("caught an exception", e);
            error.set(true);
            if (latch != null) {
              latch.countDown();
            }
          }

          @Override
          public void onComplete(testWrite_call re) {
            try {
              PerformanceTestResponseThrift response = re.getResult();
              Validate.isTrue(response.getData().length == writeSize);
            } catch (Exception e) {
              logger.error("caught an exception", e);
              error.set(true);
            } finally {
              if (latch != null) {
                latch.countDown();
              }
            }
          }
        };

        client.testWrite(request, callback);
        latch.await();
        if (error.get()) {
          logger.error("meet error");
          break;
        }
      } catch (Exception e) {
        logger.info("caught an exception", e);
        return;
      }
    }
  }

  private static class CommondLineArgs {

    public static final String SYNCHRONIZE = "--synchronize";
    public static final String READSIZE = "--readSize";
    public static final String WRITESIZE = "--writeSize";
    public static final String WORKTHREADCOUNT = "--workerThreadCount";
    public static final String WORKPOOLCOUNT = "--workPoolCount";
    public static final String SERVERADDRESS = "--serverAddress";
    public static final String RESPONSETHREADCOUNT = "--responseThreadCount";
    public static final String TESTTYPE = "--testType";
    @Parameter(names = SYNCHRONIZE, description = "", required = false)
    public boolean synchronize = false;
    @Parameter(names = READSIZE, description = "", required = false)
    public int readSize = 4096;
    @Parameter(names = WRITESIZE, description = "", required = false)
    public int writeSize = 4096;
    @Parameter(names = WORKTHREADCOUNT, description = "", required = false)
    public int workerThreadCount = 1;
    @Parameter(names = WORKPOOLCOUNT, description = "", required = false)
    public int workPoolCount = 1;
    @Parameter(names = SERVERADDRESS, description = "", required = false)
    public String serverAddress = "33333";
    @Parameter(names = RESPONSETHREADCOUNT, description = "", required = false)
    public int responseThreadCount = Runtime.getRuntime().availableProcessors() * 2;
    @Parameter(names = TESTTYPE, description = "", required = false)
    public String testType = "pingpang";
  }
}
