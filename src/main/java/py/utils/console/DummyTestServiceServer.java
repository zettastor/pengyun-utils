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

package py.utils.console;

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
import py.common.struct.EndPointParser;
import py.instance.PortType;
import py.thrift.infocenter.service.ReserveVolumeRequest;
import py.thrift.infocenter.service.ReserveVolumeResponse;
import py.thrift.testing.service.DummyTestService;
import py.thrift.testing.service.PingRequest;
import py.thrift.testing.service.TestInternalErrorThrift;

/**
 * xx.
 */
public class DummyTestServiceServer {

  private static final Logger logger = LoggerFactory.getLogger(DummyTestServiceServer.class);
  private static final String HELP_CONTENT = "--latency l --randomize r --responsesize <rs>";
  private static int SERVER_LISTEN_PORT = 33333;

  /**
   * xx.
   */
  public static String bytesTolongsString(byte[] buffer) {
    // convert byte array to string
    ByteBuffer temp = ByteBuffer.wrap(buffer, 0, buffer.length);
    String str = "";
    for (int i = 0; i < buffer.length / 8; i++) {
      str += Long.toString(temp.getLong());
      str += "=";
    }

    return str;
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {

    final CommondLineArgs dirArgs = new CommondLineArgs();
    try {
      JCommander commander = new JCommander(dirArgs, args);
    } catch (ParameterException e) {
      System.out.println(HELP_CONTENT);
      return;
    }

    final Random random = new Random(System.currentTimeMillis());
    final int latency = dirArgs.latency;
    final boolean randomizeLatency = Boolean.parseBoolean(dirArgs.randomize);
    final boolean responsesize = Boolean.parseBoolean(dirArgs.responsesize);
    byte[] bytes = new byte[128 * 1024];
    final String strBack = new String(bytes);
    if (dirArgs.listenPort == 0) {
      dirArgs.listenPort = SERVER_LISTEN_PORT;
    }

    System.out
        .println("latency: " + latency + ", randomize: " + randomizeLatency + ", responsesize: "
            + responsesize
            + ", workerThreadCount: " + dirArgs.workerThreadCount + ", listenPort: "
            + dirArgs.listenPort);

    // build a server processor
    ThriftAppEngine engine = new ThriftAppEngine(new ThriftProcessorFactory() {
      @Override
      public TProcessor getProcessor() {
        return new DummyTestService.Processor<>(new DummyTestService.Iface() {

          @Override
          public String ping(PingRequest request) throws TException {
            if (latency != 0) {
              int realLatency = latency;
              if (randomizeLatency) {
                realLatency = random.nextInt(latency);
              }

              if (realLatency != 0) {
                try {
                  Thread.sleep(realLatency);
                } catch (InterruptedException e) {
                  logger.error("interrupted ", e);
                }
              }
            }

            byte[] data = request.getData();
            if (data == null) {
              return "i am coming";
            } else {
              return bytesTolongsString(data);
            }
          }

          @Override
          public ReserveVolumeResponse reserveVolume(ReserveVolumeRequest request)
              throws TestInternalErrorThrift, TException {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public void pingforcoodinator() throws TException {
            // TODO Auto-generated method stub

          }
        });
      }
    });

    if (dirArgs.workerThreadCount != 0) {
      engine.setMinNumThreads(dirArgs.workerThreadCount);
    }

    AppContextImpl appContext = new AppContextImpl("DummyTestService_Server");
    appContext.putEndPoint(PortType.CONTROL,
        EndPointParser
            .parseLocalEndPoint(dirArgs.listenPort, InetAddress.getLocalHost().getHostAddress()));
    engine.setContext(appContext);

    // start server to supply service
    try {
      engine.start();
    } catch (Exception e) {
      System.out
          .println(
              "can not start a dummy test service on port: " + dirArgs.listenPort + ", exception: "
                  + e);
      System.exit(1);
    }

    System.out.println(
        "successfully start a dummy service on port: " + dirArgs.listenPort + ", latency: "
            + latency
            + ", randomizeLatency: " + randomizeLatency);
  }

  private static class CommondLineArgs {

    public static final String LENTENCY = "--latency";
    public static final String RANDOMIZE = "--randomize";
    public static final String RESPONSESIZE = "--responsesize";
    public static final String WORKERTHREADCOUT = "--workerThreadCount";
    public static final String LISTENPORT = "--listenPort";
    @Parameter(names = LENTENCY, description = "", required = false)
    public int latency;
    @Parameter(names = RANDOMIZE, description = "", required = false)
    public String randomize;
    @Parameter(names = RESPONSESIZE, description = "", required = false)
    public String responsesize;
    @Parameter(names = WORKERTHREADCOUT, description = "", required = false)
    public int workerThreadCount;
    @Parameter(names = LISTENPORT, description = "", required = false)
    public int listenPort;
  }
}
