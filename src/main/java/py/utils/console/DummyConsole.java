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

package py.utils.console;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.common.struct.EndPoint;
import py.common.struct.EndPointParser;
import py.storage.StorageConfiguration;
import py.thrift.icshare.CreateVolumeResponse;
import py.thrift.share.CreateAccountResponse;
import py.thrift.share.DriverTypeThrift;
import py.thrift.share.LaunchDriverResponseThrift;

/**
 * this class is a dummy console used to create a volume by given name, size, and controlcenter
 * endpoint run like this:java -cp /.* py.utils.console.DummyConsole --volumeSize size --volumeName
 * name --controlCenter endpoint --infoCenter endpoint
 */

@Configuration
@Import({StorageConfiguration.class})
public class DummyConsole {

  private static final String HELP_CONTENT = "--volumeSize size --controlCenter" 
      + " cl --infoCenter ic --volumeName vn";

  @Autowired
  private StorageConfiguration storageConfiguration;

  /**
   * xx.
   */
  public static void launchDriver(ConsoleCenter console, long volumeId, String volumeName,
      long accountId,
      String driveType) {
    LaunchDriverResponseThrift launchDriverResponse = console
        .launchDriver(volumeId, volumeName, accountId,
            DriverTypeThrift.valueOf(driveType));
    System.out.println("Driver is " + driveType);
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    CommondLineArgs dirArgs = new CommondLineArgs();
    try {
      JCommander commander = new JCommander(dirArgs, args);
    } catch (ParameterException e) {
      System.out.println(HELP_CONTENT);
      return;
    }

    long volumeSize = Long.parseLong(dirArgs.requestVolumeSize);
    EndPoint controlEp = EndPointParser.parseInSubnet(dirArgs.requestControlCenter, dirArgs.subnet);
    EndPoint infoEp = EndPointParser.parseInSubnet(dirArgs.requestInfoCenter, dirArgs.subnet);

    ApplicationContext ctx = new AnnotationConfigApplicationContext(DummyConsole.class);
    DummyConsole dummyConsole = ctx.getBean(DummyConsole.class);
    dummyConsole.createVolumeAndLaunchIt(controlEp, infoEp, volumeSize, dirArgs.requestVolumeName,
        dirArgs.driverType);
  }

  /**
   * xx.
   */
  public void createVolumeAndLaunchIt(EndPoint controlEp, EndPoint infoEp, long volumeSize,
      String volumeName,
      String driverType) {
    long segmentSizeByte = storageConfiguration.getSegmentSizeByte();
    if (volumeSize < segmentSizeByte) {
      System.err.println("volume size " + volumeSize
          + " is not correct, please input a size that is larger than a segment size "
          + segmentSizeByte);
      System.exit(1);
    } else if (volumeSize % storageConfiguration.getSegmentSizeByte() != 0) {
      System.err.println("volume size " + volumeSize
          + " is not correct, please input multiple times of a segment size " + segmentSizeByte);
      System.exit(1);
    }

    ConsoleCenter console = null;

    try {
      console = new ConsoleCenter();
      console.setControlEps(controlEp);
      console.setInfoEps(infoEp);

      CreateVolumeResponse createVolumeResponse = null;
      CreateAccountResponse createAccountResponse = null;

      createAccountResponse = console
          .createAccount("u" + System.currentTimeMillis(), "dummypassword",
              Constants.AccountId4IntegTest);
      createVolumeResponse = console
          .createVolume(volumeName, volumeSize, createAccountResponse.getAccountId(),
              Constants.VolumeId4IntegTest);

      System.out.println("AccountId: " + createAccountResponse.getAccountId());
      System.out.println("VolumeSize: " + volumeSize);
      System.out.println("VolumeId: " + createVolumeResponse.getVolumeId());
      if (driverType != null) {
        launchDriver(console, createVolumeResponse.getVolumeId(), volumeName,
            createAccountResponse.getAccountId(), driverType.toUpperCase());
      }
    } catch (Exception e) {
      System.err.println("Can't create a volume. caught an exception");
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (console != null) {
        console.close();
      }
    }
  }

  private static class CommondLineArgs {

    public static final String REQUEST_VOLUME_SIZE = "--volumeSize";
    public static final String CONTROL_CENTER = "--controlCenter";
    public static final String INFO_CENTER = "--infoCenter";
    public static final String REQUEST_VOLUME_NAME = "--volumeName";
    public static final String Driver_TYPE = "--driverType";
    public static final String SUBNET = "--subnet";
    public static final String HELP = "--help";
    @Parameter(names = REQUEST_VOLUME_SIZE, description = "", required = true)
    public String requestVolumeSize;
    @Parameter(names = CONTROL_CENTER, description = "", required = true)
    public String requestControlCenter;
    @Parameter(names = INFO_CENTER, description = "", required = true)
    public String requestInfoCenter;
    @Parameter(names = REQUEST_VOLUME_NAME, description = "", required = true)
    public String requestVolumeName;
    @Parameter(names = Driver_TYPE, description = "", required = false)
    public String driverType;
    @Parameter(names = SUBNET, description = "", required = true)
    public String subnet;
    @Parameter(names = HELP, help = true)
    private boolean help;
  }
}
