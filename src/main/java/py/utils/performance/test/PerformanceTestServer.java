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
import com.beust.jcommander.ParameterException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContextImpl;
import py.app.thrift.ThriftAppEngine;
import py.app.thrift.ThriftProcessorFactory;
import py.common.struct.EndPoint;
import py.instance.PortType;
import py.thrift.testing.service.PerformanceTestExceptionThrift;
import py.thrift.testing.service.PerformanceTestRequestThrift;
import py.thrift.testing.service.PerformanceTestResponseThrift;
import py.thrift.testing.service.PerformanceTestService;

/**
 * xx.
 */
public class PerformanceTestServer {

  private static final Logger logger = LoggerFactory.getLogger(PerformanceTestServer.class);
  private static final String HELP_CONTENT =
      "--latency <0> --random <false> --readSize <4096> --workerThreadCount <> "
          + "--listenPort <33333> --requestThreadCount <twice of cpu>";

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    final CommondLineArgs cla = new CommondLineArgs();
    try {
      @SuppressWarnings("unused")
      JCommander commander = new JCommander(cla, args);
    } catch (ParameterException e) {
      System.out.println(HELP_CONTENT);
      return;
    }

    final Random random = new Random(System.currentTimeMillis());

    System.out.println(
        "latency: " + cla.latency + ", randomize: " + cla.random + ", readSize: " + cla.readSize
            + ", workerThreadCount: " + cla.workerThreadCount + ", listenPort: " + cla.listenPort
            + ", requestThreadCount: " + cla.requestThreadCount);
    /*
     * build a server processor
     */
    ThriftAppEngine engine = new ThriftAppEngine(new ThriftProcessorFactory() {

      @Override
      public TProcessor getProcessor() {
        return new PerformanceTestService.Processor<>(new PerformanceTestService.Iface() {

          @Override
          public PerformanceTestResponseThrift testPingPang(PerformanceTestRequestThrift request)
              throws PerformanceTestExceptionThrift, TException {

            PerformanceTestResponseThrift response = new PerformanceTestResponseThrift();
            response.setRequestId(request.getRequestId());
            response.setData(request.getData());
            if (cla.latency != 0) {
              try {
                if (cla.random) {
                  Thread.sleep(random.nextInt(cla.latency));
                } else {
                  Thread.sleep(cla.latency);
                }
              } catch (Exception e) {
                logger.info("caught an exception", e);
              }
            }
            return response;
          }

          @Override
          public PerformanceTestResponseThrift testRead(PerformanceTestRequestThrift request)
              throws PerformanceTestExceptionThrift, TException {
            PerformanceTestResponseThrift response = new PerformanceTestResponseThrift();
            response.setRequestId(request.getRequestId());
            ByteBuffer byteBuffer = ByteBuffer.wrap(request.getData());
            int size = byteBuffer.getInt();
            response.setData(new byte[size]);
            return response;
          }

          @Override
          public PerformanceTestResponseThrift testWrite(PerformanceTestRequestThrift request)
              throws PerformanceTestExceptionThrift, TException {
            PerformanceTestResponseThrift response = new PerformanceTestResponseThrift();
            response.setRequestId(request.getRequestId());
            ByteBuffer byteBuffer = ByteBuffer.wrap(request.getData());
            response.setData(byteBuffer.array());
            return response;
          }

        });
      }
    });

    AppContextImpl appContext = new AppContextImpl("PerformanceTestServiceServer");
    appContext.putEndPoint(PortType.CONTROL,
        new EndPoint(InetAddress.getLocalHost().getHostAddress(), cla.listenPort));
    engine.setContext(appContext);

    try {
      engine.start();
    } catch (Exception e) {
      System.out.println(
          "can not start a performance test service on port: " + cla.listenPort + ", exception: "
              + e);
      System.exit(1);
    }
  }

  private static class CommondLineArgs {

    public static final String LENTENCY = "--latency";
    public static final String RANDOM = "--random";
    public static final String READSIZE = "--readSize";
    public static final String WORKERTHREADCOUNT = "--workerThreadCount";
    public static final String LISTENPORT = "--listenPort";
    public static final String REQUESTTHREADCOUNT = "--requestThreadCount";
    @Parameter(names = LENTENCY, description = "", required = false)
    public int latency;
    @Parameter(names = RANDOM, description = "", required = false)
    public boolean random;
    @Parameter(names = READSIZE, description = "", required = false)
    public int readSize = 4096;
    @Parameter(names = WORKERTHREADCOUNT, description = "", required = false)
    public int workerThreadCount = 1;
    @Parameter(names = LISTENPORT, description = "", required = false)
    public int listenPort = 33333;
    @Parameter(names = REQUESTTHREADCOUNT, description = "", required = false)
    public int requestThreadCount = Runtime.getRuntime().availableProcessors() * 2;

  }
}
