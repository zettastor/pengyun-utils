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

package py.database.test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.DeploymentDaemonClientFactory;
import py.common.Constants;
import py.common.struct.EndPoint;
import py.dd.DeploymentDaemonClientHandler;
import py.dd.common.ServiceStatus;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;
import py.icshare.VolumeInformation;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.Instance;
import py.thrift.icshare.GetVolumeRequest;
import py.thrift.icshare.GetVolumeResponse;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.SegmentMetadataThrift;
import py.thrift.share.SegmentUnitMetadataThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.VolumeMetadataThrift;
import py.thrift.share.VolumeStatusThrift;

/**
 * this class test the database IO when some of the datanodes down.
 */
public class DatabaseTests {

  private static final Logger logger = LoggerFactory.getLogger(DatabaseTests.class);
  private static final String IPADDRESS_PATTERN =
      "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
          + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
  private final int readThreadNum = 5;
  private final int writeThreadNum = 3;
  private long latestCheckedTime;
  private long longestTimeLapse = 0;
  private String infoCenterHostName;
  private long fakedVolumeId;
  private String weigh;
  private int timeout = 10000000;
  private BlockingQueue<VolumeInfoToCheck> checkQueue;
  private int round = 1;

  /**
   * xx.
   */
  public DatabaseTests() {
    this.fakedVolumeId = 0L;
    this.weigh = new String("00");
    this.checkQueue = new LinkedBlockingQueue<VolumeInfoToCheck>();
    while (weigh.length() < 30000) {
      weigh = weigh + weigh;
    }
    logger.info("row length {}", weigh.length() * 4);
  }

  /**
   * xx.
   */
  public static void usage() {
    System.out.println(
        "usage: \n\tjava -jar databaseTest.jar DataNodeHostNames "
            + "InfoCenterHostName VolumeId [once]");
    System.out.println("\tOR:");
    System.out.println("\tjava -jar databaseTest.jar skipKill");
    System.out.println(
        "example: \n\tjava -jar databaseTest.jar 10.0.1.140:10.0.1.143 10.0.1.140 12345678");
    System.out.println(
        "\tOR:\n\tjava -jar databaseTest.jar 10.0.1.140:10.0.1.143 10.0.1.140 12345678 once");
    System.out.println("\tOR:\n\tjava -jar databaseTest.jar skipKill");
    System.out.println("ATTENTION:");
    System.out.println(
        "\tThe program runs forever unless you say once in the arguements or test failed!!");
    System.out.println("\tYou should specify at least 3 datanodes for the test!!");
    System.out.println(
        "\tPlease make sure you have the /src/main/resources/hibernate.cfg.xml "
            + "file changed\n\tAnd COMPILE the project AFTER that!!");
    System.exit(-1);
  }

  private static List<String> getIps(String ips) {
    Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
    List<String> ipParts = new ArrayList<String>();
    if (ips.contains(",")) {
      String[] ipPartsArray = ips.split(",");
      for (String ipPart : ipPartsArray) {
        ipParts.add(ipPart);
      }
    } else {
      ipParts.add(ips);
    }
    List<String> ipList = new ArrayList<String>();
    for (String ipPart : ipParts) {
      if (!ipPart.contains(":")) {
        Matcher matcher = pattern.matcher(ipPart);
        if (!matcher.matches()) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        } else {
          ipList.add(ipPart);
          break;
        }
      }
      String[] ip = ipPart.split(":");
      if (ip.length != 2) {
        System.out.println("Are you kidding me? Correct ip address OK ?!");
        usage();
      }
      for (String eachIp : ip) {
        Matcher matcher = pattern.matcher(eachIp);
        if (!matcher.matches()) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        }
      }
      String[] ipUnit1 = ip[0].split("\\.");
      String[] ipUnit2 = ip[1].split("\\.");
      for (int i = 0; i < 3; i++) {
        if (!ipUnit1[i].endsWith(ipUnit2[i])) {
          System.out.println("Are you kidding me? Correct ip address OK ?!");
          usage();
        }
      }
      int start = Integer.parseInt(ipUnit1[3]);
      int end = Integer.parseInt(ipUnit2[3]);
      if (start > end) {
        int tmp = start;
        start = end;
        end = tmp;
      }
      for (int i = start; i <= end; i++) {
        ipList.add(ipUnit1[0] + "." + ipUnit1[1] + "." + ipUnit1[2] + "." + String.valueOf(i));
      }
    }
    return ipList;
  }

  /**
   * xx.
   */
  public static void main(String[] args) {

    DatabaseTests databaseTests = new DatabaseTests();

    databaseTests.startJob(args);
  }

  private synchronized VolumeInformation fakeVolume() {
    VolumeInformation volume = new VolumeInformation();
    volume.setVolumeId(fakedVolumeId);
    Long timeStamp = System.currentTimeMillis();
    volume.setRootVolumeId(timeStamp);
    String name = String.valueOf(new Date(timeStamp));
    volume.setName(name);

    // make throughput larger
    volume.setVolumeStatus(weigh);
    volume.setTagKey(weigh);
    volume.setTagValue(weigh);
    volume.setVolumeType(weigh);
    fakedVolumeId++;
    return volume;
  }

  /**
   * xx.
   */
  public void startJob(String[] args) {
    if (args.length != 1 && args.length != 3 && args.length != 4) {
      usage();
    }
    latestCheckedTime = System.currentTimeMillis();
    Session session = null;
    boolean skipKill = false;
    for (String string : args) {
      if (string.equalsIgnoreCase("skipKill")) {
        skipKill = true;
      }
    }

    SessionFactory sessionFactory = HibernateUtil.getSessionFactory();

    try {
      logger.info(
          "I am going to delete all the data in database first ,"
              + " this may take a little time, please wait \nwait \nwait \n...");
      session = sessionFactory.openSession();
      String hql = "delete VolumeInformation as v where v.id>=0";
      Query query = session.createQuery(hql);
      query.executeUpdate();
      session.beginTransaction().commit();
    } catch (Exception e) {
      logger.warn(
          "cannot delete database at the beginning , maybe there is no data in database at all~");
    }

    for (int i = 0; i < readThreadNum; i++) {
      session = sessionFactory.openSession();
      startReading(session);
    }

    for (int i = 0; i < writeThreadNum; i++) {
      session = sessionFactory.openSession();
      startSaving(session);
    }

    if (!skipKill) {
      List<String> datanodes = getIps(args[0]);

      if (datanodes.size() < 3) {
        logger.error("if you want to kill datanodes, you should give me at least 3 datanodes");
        usage();
      }

      logger.info("datanode hosts: {}", datanodes);

      infoCenterHostName = args[1];
      Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
      ;
      Matcher matcher = pattern.matcher(infoCenterHostName);
      if (!matcher.matches()) {
        usage();
      }
      logger.info("infocenter hosts: {}", infoCenterHostName);
      Long volumeId = null;
      try {
        volumeId = Long.parseLong(args[2]);
      } catch (Exception e) {
        usage();
      }
      logger.info("volumeId: {}", volumeId);

      boolean once = false;
      if (args.length == 4) {
        if (!args[3].equalsIgnoreCase("once")) {
          usage();
        }
        once = true;
      }

      try {
        Thread.sleep(5000);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }

      try {
        startKill(datanodes, volumeId, once);
      } catch (Exception e) {
        logger.error("cannot start killing thread", e);
        System.exit(-1);
      }
    }

    long lastCheckFromNow;
    while (true) {
      lastCheckFromNow = System.currentTimeMillis() - latestCheckedTime;
      if (lastCheckFromNow > longestTimeLapse) {
        longestTimeLapse = lastCheckFromNow;
      }
      if (lastCheckFromNow > timeout) {
        logger.error(
            "DB write timeout 120 seconds! longest time lapse: "
                + "{} mill seconds, you want to change the timeout? read my codes.",
            longestTimeLapse);
        System.exit(-1);
      } else {
        logger.info(
            "Status good, round {}, last check from now: "
                + "{} mill seconds longest time lapse till now: {}",
            round, lastCheckFromNow, longestTimeLapse);
      }
      try {
        Thread.sleep(5000);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

  }

  private void startSaving(Session session) {
    Thread saveThread = new Thread(new SaveDatabaseThread(session), "write-database-thread");
    saveThread.start();
  }

  private void startReading(Session session) {
    Thread readThread = new Thread(new ReadDatabaseThread(session), "read-database-thread");
    readThread.start();
  }

  private void startKill(List<String> datanodes, Long volumeId, boolean once) {
    Thread killThread = new Thread(
        new KillDataNodeAndCheckIoStatusThread(datanodes, infoCenterHostName, volumeId, once),
        "kill-datanode-thread");
    killThread.start();
  }

  private VolumeInfoToCheck poll(long timeout) {
    try {
      return checkQueue.poll(timeout, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private class ReadDatabaseThread implements Runnable {

    private Session session;

    public ReadDatabaseThread(Session session) {
      super();
      this.session = session;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      logger.info("read thread start");
      int count = 0;
      while (true) {
        VolumeInfoToCheck volumeInfo = poll(1000);
        if (volumeInfo == null) {
          continue;
        } else {
          List<String> volumeName = session.createQuery(
              "select name from VolumeInformation where volumeId=" + volumeInfo.volumeId).list();
          if (volumeName == null || volumeName.size() != 1) {
            try {
              checkQueue.put(volumeInfo);
              continue;
            } catch (InterruptedException e) {
              e.printStackTrace();
              System.exit(-1);
            }
          }
          if (!volumeName.get(0).equals(volumeInfo.volumeName)) {
            logger.error("data dismatch for volumeId {}, supposed volume name {}, but {}",
                volumeInfo.volumeId, volumeName.get(0), volumeInfo.volumeName);
            System.exit(-1);
          } else {
            latestCheckedTime = System.currentTimeMillis();
            count++;
          }
        }
        if (count >= 100) {
          logger.info("{} rows checked data matches, latest check {}, queue size {}", count,
              volumeInfo.volumeName, checkQueue.size());
          count = 0;
        }
      }
    }
  }

  private class SaveDatabaseThread implements Runnable {

    private Session session;

    public SaveDatabaseThread(Session session) {
      super();
      this.session = session;
    }

    @Override
    public void run() {
      logger.info("write thread start");
      while (true) {
        Random ran = new Random();
        int counts = ran.nextInt(99);
        counts += 100;

        try {
          List<VolumeInfoToCheck> volumeInfos = new ArrayList<VolumeInfoToCheck>();
          session.beginTransaction();
          for (int i = 0; i < counts; i++) {
            VolumeInformation volume = fakeVolume();
            session.save(volume);
            volumeInfos.add(new VolumeInfoToCheck(volume));
          }
          session.getTransaction().commit();
          checkQueue.addAll(volumeInfos);
          logger.info("{} rows saved to database", counts);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(-1);
        }

        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(-1);
        }

      }

    }

  }

  class KillDataNodeAndCheckIoStatusThread implements Runnable {

    private final int deploymentDaemonPort = 10002;
    private final String datanodeServiceName = "DataNode";
    private final String dihEndpoint = "10000";
    private final String infocenterEndpoint = "8020";
    private Long volumeId;
    private List<String> dataNodeHostNames;
    private String infoCenterHostName;
    private DeploymentDaemonClientHandler deploymentDaemonClientHandler;

    private DihClientFactory dihClientFactory = new DihClientFactory(1);

    private InformationCenterClientFactory infoCenterClientFactory =
        new InformationCenterClientFactory(
            1);

    private boolean once;

    public KillDataNodeAndCheckIoStatusThread(List<String> dataNodeHostNames,
        String infoCenterHostName, Long volumeId, boolean once) {
      this.dataNodeHostNames = dataNodeHostNames;
      this.deploymentDaemonClientHandler = deploymentDaemonClientHandler();
      this.infoCenterHostName = infoCenterHostName;
      this.volumeId = volumeId;
      this.once = once;
    }

    @Override
    public void run() {
      while (true) {
        doWork();
        if (once) {
          System.exit(0);
        }
      }
    }

    public void doWork() {
      for (String dataNodeHost : dataNodeHostNames) {
        logger.info("Well, let me check if your datanodes are all active");
        ensureDataNodeStatus(dataNodeHost);
      }
      if (dataNodeHostNames.size() == 3) {
        for (String dataNodeHost : dataNodeHostNames) {
          waitVolumeToBeAvailable(true);
          deactiveDataNode(dataNodeHost, false);
          activeDataNode(dataNodeHost, true);
        }
        logger.info("\n\n\n\ntest passed, the longest time lapse during the test is : {}\n\n\n",
            longestTimeLapse);
        round++;
        return;
      } else {
        String dataNodeHostName = pickUpOneKillableDatanode(dataNodeHostNames);
        // test case 1
        waitVolumeToBeAvailable(true);
        deactiveDataNode(dataNodeHostName, true);
        // test case 2
        for (String dataNodeHost : dataNodeHostNames) {
          if (dataNodeHost.equals(dataNodeHostName)) {
            continue;
          }
          waitVolumeToBeAvailable(true);
          deactiveDataNode(dataNodeHost, false);
          activeDataNode(dataNodeHost, true);
        }
        activeDataNode(dataNodeHostName, true);
        logger.info("\n\n\ntest passed, the longest time lapse during the test is : {}\n\n",
            longestTimeLapse);
        return;
      }
      // System.exit(0);
    }

    private void ensureDataNodeStatus(String dataNodeHost) {
      try {
        ServiceStatus status = deploymentDaemonClientHandler
            .getStatus(dataNodeHost, deploymentDaemonPort, datanodeServiceName)
            .getServiceStatus();
        if (status != ServiceStatus.ACTIVE) {
          logger.error(
              "BAD BOY!! datanode on {} is {} ! Go back and double "
                  + "check it, and the system will exit",
              dataNodeHost, status);
          System.exit(-1);
        } else {
          logger.info("GOOD , datanode on {} is {}", dataNodeHost, status);
        }
      } catch (Exception e) {
        logger.error("Exception catch:", e);
        System.exit(-1);
      }

    }

    public void deactiveDataNode(String dataNodeHostName, boolean pss) {
      try {
        logger.info("\n\n\n\nnow I am going to shutdown datanode {} \n\n\n", dataNodeHostName);
        if (deploymentDaemonClientHandler
            .deactivate(dataNodeHostName, deploymentDaemonPort, datanodeServiceName, 0,
                false)) {
          waitDataNodeToBe(dataNodeHostName, ServiceStatus.DEACTIVE);
          waitVolumeToBeAvailable(pss);
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    public void activeDataNode(String dataNodeHostName, boolean pss) {
      try {
        logger.info("\n\n\n\nnow I am going to start datanode {} \n\n\n", dataNodeHostName);
        if (deploymentDaemonClientHandler
            .start(dataNodeHostName, deploymentDaemonPort, datanodeServiceName)) {
          waitDataNodeToBe(dataNodeHostName, ServiceStatus.ACTIVE);
          logger.info("datanode on {} is active now let's continue", dataNodeHostName);
          waitVolumeToBeAvailable(pss);
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    public void waitDataNodeToBe(String hostName, ServiceStatus serviceStatus) throws Exception {
      int waitedTime = 0;
      while (true) {
        ServiceStatus status = deploymentDaemonClientHandler
            .getStatus(hostName, deploymentDaemonPort, datanodeServiceName).getServiceStatus();
        if (status != serviceStatus) {
          logger.info(
              "wait datanode on {} to be {}, current status:"
                  + " {} let's wait 2 seconds, already waited {} seconds",
              hostName, serviceStatus.name(), status.name(), waitedTime);
          if (waitedTime > 600) {
            logger.error("\n\n\n\nTIMEOUT!!!!!!!!!\n\n\n");
            System.exit(-1);
            restart(hostName);
          }
          try {
            waitedTime += 2;
            Thread.sleep(2000);
          } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
          }
        } else {
          return;
        }
      }
    }

    private void restart(String hostName) {
      final String cmd = "/kill.pl " + hostName;
      try {
        Process pid = Runtime.getRuntime().exec(cmd);
        if (pid != null) {
          pid.waitFor();
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

    public void waitVolumeToBeAvailable(boolean pss) {
      int timewaited = 0;
      while (true) {
        VolumeMetadataThrift volume = getVolume();
        if (checkAllSegUnitOk(volume, pss)) {
          logger.info("Volume Status is completely OK, let's continue");
          return;
        }
        logger.info(
            "Volume Status is not completely OK, "
                + "let's wait 3 seconds and try again waited {} seconds",
            timewaited);
        try {
          timewaited += 3;
          Thread.sleep(3000);
        } catch (Exception e) {
          logger.error("Exception catch", e);
          System.exit(-1);
        }
      }
    }

    public VolumeMetadataThrift getVolume() {
      InformationCenter.Iface client = null;
      try {
        client = infoCenterClientFactory
            .build(new EndPoint(infoCenterHostName + ":" + infocenterEndpoint)).getClient();
        GetVolumeRequest request = new GetVolumeRequest();
        request.setVolumeId(volumeId);
        request.setAccountId(Constants.SUPERADMIN_ACCOUNT_ID);
        GetVolumeResponse response = client.getVolume(request);
        VolumeMetadataThrift volume = null;
        if (response.isSetVolumeMetadata()) {
          volume = response.getVolumeMetadata();
        }
        return volume;
      } catch (Exception e) {
        logger.error("Exception catch", e);
        System.exit(-1);
      }
      return null;
    }

    public boolean checkAllSegUnitOk(VolumeMetadataThrift volume, boolean pss) {
      if (volume.getVolumeStatus() != VolumeStatusThrift.Available
          || volume.getVolumeStatus() != VolumeStatusThrift.Stable) {
        return false;
      }
      for (SegmentMetadataThrift segment : volume.getSegmentsMetadata()) {
        int pcount = 0;
        int scount = 0;
        for (SegmentUnitMetadataThrift segUnit : segment.getSegmentUnits()) {
          if (segUnit.getStatus() == SegmentUnitStatusThrift.Primary) {
            pcount++;
          }
          if (segUnit.getStatus() == SegmentUnitStatusThrift.Secondary) {
            scount++;
          }
        }
        if (pcount != 1) {
          return false;
        }
        if (pss ? (scount != 2) : (scount != 1)) {
          return false;
        }
      }
      return true;
    }

    public String pickUpOneKillableDatanode(List<String> dataNodeHosts) {
      Map<Integer, List<String>> dataNodeGroup = new HashMap<Integer, List<String>>();
      Map<Long, String> dataNodeId2Ip = new HashMap<Long, String>();
      Set<Instance> instances = null;
      VolumeMetadataThrift volume = getVolume();
      try {
        DihServiceBlockingClientWrapper dihWrapper = dihClientFactory
            .build(new EndPoint(dataNodeHosts.get(0) + ":" + dihEndpoint));
        instances = dihWrapper.getInstanceAll();
      } catch (Exception e) {
        logger.error("Exception catch {} {}", e);
        System.exit(-1);
      }
      for (String dataNodeHost : dataNodeHosts) {
        if (instances != null && instances.size() > 0) {
          for (Instance instance : instances) {
            if (instance.getName().equals("DataNode") && instance.getEndPoint().getHostName()
                .equals(dataNodeHost)) {
              if (!dataNodeId2Ip.containsKey(instance.getId().getId())) {
                dataNodeId2Ip.put(instance.getId().getId(), instance.getEndPoint().getHostName());
              }
              logger.error("A datanode with group {}", instance.getGroup().getGroupId());
              ;
              if (dataNodeGroup.containsKey(instance.getGroup().getGroupId())) {
                dataNodeGroup.get(instance.getGroup().getGroupId()).add(dataNodeHost);
              } else {
                List<String> dataNodes = new ArrayList<String>();
                dataNodes.add(dataNodeHost);
                dataNodeGroup.put(instance.getGroup().getGroupId(), dataNodes);
              }
            }
          }
        }
      }
      for (Entry<Integer, List<String>> entry : dataNodeGroup.entrySet()) {
        if (entry.getValue().size() > 1) {
          for (SegmentMetadataThrift segment : volume.getSegmentsMetadata()) {
            for (SegmentUnitMetadataThrift segUnit : segment.getSegmentUnits()) {
              for (String hosts : entry.getValue()) {
                if (dataNodeId2Ip.get(segUnit.getInstanceId()).equals(hosts)) {
                  return hosts;
                }
              }
            }
          }
        }
      }
      return null;
    }

    public DeploymentDaemonClientHandler deploymentDaemonClientHandler() {
      DeploymentDaemonClientHandler deploymentDaemonClient = new DeploymentDaemonClientHandler();
      deploymentDaemonClient.setDeploymentDaemonClientFactory(new DeploymentDaemonClientFactory(1));
      return deploymentDaemonClient;
    }

  }

  class VolumeInfoToCheck {

    long volumeId;
    String volumeName;

    VolumeInfoToCheck(long volumeId, String volumeName) {
      this.volumeId = volumeId;
      this.volumeName = volumeName;
    }

    VolumeInfoToCheck(VolumeInformation volume) {
      this.volumeId = volume.getVolumeId();
      this.volumeName = volume.getName();
    }
  }

}