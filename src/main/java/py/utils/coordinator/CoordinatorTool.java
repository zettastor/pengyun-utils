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

package py.utils.coordinator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.coordinator.FakeStorage;
import py.coordinator.configuration.CoordinatorConfigSingleton;
import py.coordinator.configuration.NbdConfiguration;
import py.coordinator.lib.Coordinator;
import py.storage.Storage;
import py.utils.test.ConsoleLogger;

/**
 * xx.
 */
public class CoordinatorTool {

  private static final Logger logger = LoggerFactory.getLogger(CoordinatorTool.class);
  private static final long startTime = System.currentTimeMillis();
  private static final BlockingQueue<DataWrapper> waitForWritingQueue =
      new LinkedBlockingQueue<DataWrapper>();
  private static final BlockingQueue<DataWrapper> waitForReadingQueue =
      new LinkedBlockingQueue<DataWrapper>();
  private static final AtomicLong readCounter = new AtomicLong(0);
  private static final AtomicLong writeCounter = new AtomicLong(0);
  private static long volumeId = RequestIdBuilder.get();
  private static int snapshotId = 0;
  private static ExitStatus exitStatus = ExitStatus.NOT_EXIT;
  private static RoundFileManager roundFileManager;
  private static DataNodeManager dataNodeManager;

  private static Storage storage;
  private static CoordinatorToolConfiguration coordinatorToolConfiguration =
      new CoordinatorToolConfiguration();

  private static String SNAPSHOT_NAME_PREFIX = "snapshot_name_";

  private static Coordinator coordinator = null;
  private static List<Thread> readThreads = new ArrayList<Thread>();
  private static List<Thread> writeThreads = new ArrayList<Thread>();
  private static Thread genreatePlanThread;
  private static Semaphore syncGenerateWithWrite = new Semaphore(0);

  private static void initCoordinatorConfiguration() {
    CoordinatorConfigSingleton cfg = CoordinatorConfigSingleton.getInstance();
    cfg.setPageSize(coordinatorToolConfiguration.getPageSize());
    cfg.setSegmentSize(coordinatorToolConfiguration.getSegmentBytesSize());
    cfg.setPageWrappedCount(coordinatorToolConfiguration.getPageWrapperCount());
    cfg.setReadCacheForIo(512);
    cfg.setPageCacheForRead(4096);
    ConsoleLogger.log("CoordinatorConfiguration =" + cfg);
  }

  /**
   * xx.
   */
  public static void main(String[] args) throws Exception {
    try {
      JCommander commander = new JCommander(coordinatorToolConfiguration, args);
      if (coordinatorToolConfiguration.isHelp()) {
        commander.usage();
      }
    } catch (ParameterException e) {
      System.out.println("caught an exception, cause=" + e);
      JCommander commander = new JCommander(coordinatorToolConfiguration, new String[0]);
      commander.usage();
      return;
    }

    LoggerConfiguration loggerConfiguration = new LoggerConfiguration();
    loggerConfiguration.setLevel(coordinatorToolConfiguration.getDebugLevel());
    loggerConfiguration.init();

    logger.warn("config={}, segment size={}", coordinatorToolConfiguration);
    coordinatorToolConfiguration.validate();

    try {
      work();
    } catch (Throwable e) {
      logger.error("caught an exception", e);
    }

    try {
      if (genreatePlanThread != null) {
        genreatePlanThread.join();
      }
    } catch (Exception e) {
      logger.error("can not shutdown generate plan thread={}", genreatePlanThread, e);
    }

    // wait for write-thread exit
    for (Thread thread : writeThreads) {
      try {
        thread.join();
      } catch (Exception e) {
        logger.error("can not shutdown write thread={}", thread, e);
      }
    }

    // wait for read-thread exit
    for (Thread thread : readThreads) {
      try {
        thread.join();
      } catch (Exception e) {
        logger.error("can not read thread={}", thread, e);
      }
    }

    // now Read all written data and check all written data
    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.error("fail to close storage={}", storage, e);
    }

    try {
      VolumeUtils.close();
    } catch (Exception e) {
      logger.error("fail to close all client factory", e);
    }

    logger.warn("exit main thread, status={}", exitStatus);
    roundFileManager.flush();
  }

  private static void work() throws Exception {
    roundFileManager = new RoundFileManager();
    if (roundFileManager.existing()) {
      // prompt to give a tip that if the files in round directory can be remove
      System.out.println("Would you like to play: 'y' or 'yes' to accept; 'n' or 'no' to reject");
      Scanner scanner = new Scanner(System.in);
      try {
        while (scanner.hasNextLine()) {
          String token = scanner.nextLine().trim();
          if (token.equalsIgnoreCase("y") || token.equalsIgnoreCase("yes")) {
            System.out.println("a new round file will produce");
            break;
          } else if (token.equalsIgnoreCase("n") || token.equalsIgnoreCase("no")) {
            System.out.println("exit");
            System.exit(0);
          } else {
            System.out.println("Oops, not a valid input!");
          }
        }
      } finally {
        scanner.close();
      }
    }

    try {
      roundFileManager.create(true);
    } catch (Exception e) {
      logger.error("fail to round file manager", e);
      return;
    }

    // prepare the environment for test
    try {
      VolumeUtils.setDihEndPoint(EndPoint.fromString(coordinatorToolConfiguration.getDih()));
    } catch (Exception e) {
      logger.error("fail to get dihs", e);
      return;
    }

    // set configuration
    initCoordinatorConfiguration();

    // load all data nodes
    try {
      dataNodeManager = new DataNodeManager();
      dataNodeManager.confirmAllDataNodeOk();
    } catch (Exception e) {
      logger.error("fail to data node manager", e);
      return;
    }

    // create domain and storage pool
    try {
      VolumeUtils.createDomainAndStoragePool(coordinatorToolConfiguration.getDomainName(),
          coordinatorToolConfiguration.getStoragePoolName());
    } catch (Exception e) {
      logger.error("fail to domain and storage pool", e);
      return;
    }

    // create volume
    try {
      volumeId = VolumeUtils.createVolume(volumeId, coordinatorToolConfiguration.getVolumeName(),
          coordinatorToolConfiguration.getVolumeSize(),
          coordinatorToolConfiguration.getDomainName(),
          coordinatorToolConfiguration.getStoragePoolName());
      VolumeUtils.waitForVolumeStable(volumeId, Long.MAX_VALUE);
    } catch (Exception e) {
      logger.error("fail to create volume", e);
      return;
    }

    try {
      coordinator = VolumeUtils.getCoordinator(volumeId);
      coordinator.open();
    } catch (Exception e) {
      coordinator.close();
      logger.error("fail to generate a new coordinator", e);
      return;
    }

    storage = new FakeStorage(new NbdConfiguration(), coordinator);

    // start writing
    for (int i = 0; i < coordinatorToolConfiguration.getWriteThreadCount(); i++) {
      Thread thread = new Thread("write thread") {
        public void run() {
          try {
            startWriteData();
          } catch (Exception e) {
            logger.error("fail to write", e);
          }
        }
      };
      writeThreads.add(thread);
      thread.start();
    }

    // start reading data
    for (int i = 0; i < coordinatorToolConfiguration.getReadThreadCount(); i++) {
      Thread thread = new Thread("read thread") {
        public void run() {
          try {
            startReadData();
          } catch (Exception e) {
            logger.error("fail to write", e);
          }
        }
      };

      readThreads.add(thread);
      thread.start();
    }

    // wait for generate-plan-thread to exit
    genreatePlanThread = new Thread("generate plan thread") {
      public void run() {
        try {
          generateWritePlan();
        } catch (Exception e) {
          logger.error("fail to generate plans", e);
          exitStatus = ExitStatus.EXCEPTION_EXIT;
        }
      }
    };

    genreatePlanThread.start();
  }

  /**
   * xx.
   */
  public static void startWriteData() throws Exception {
    Random random = new Random();
    while (true) {
      DataWrapper wrapper = waitForWritingQueue.poll(1000L, TimeUnit.MILLISECONDS);
      if (exitStatus != ExitStatus.NOT_EXIT) {
        ConsoleLogger.log("exit read thread");
        break;
      }

      if (wrapper == null) {
        // just wait for next writing round.
        continue;
      }

      // write data
      int writeTimes = random.nextInt(coordinatorToolConfiguration.getWriteRepeatTimes()) + 1;
      try {
        writeData(wrapper, writeTimes, random.nextBoolean());
        writeCounter.incrementAndGet();
        syncGenerateWithWrite.release();
      } catch (Exception e) {
        exitStatus = ExitStatus.EXCEPTION_EXIT;
        logger.error("fail to write, wrapper={},write times={}", wrapper, writeTimes, e);
      }

      // add read request to read queue
      if (exitStatus == ExitStatus.NOT_EXIT) {
        waitForReadingQueue.add(wrapper);
      }
    }

    logger.warn("exit writing data, status={}", exitStatus);
  }

  private static void writeData(DataWrapper wrapper, int writeTimes, boolean split)
      throws Exception {
    long time = wrapper.getLongTime();
    for (int i = 0; i < writeTimes; i++) {
      long offset = wrapper.getOffsetSector() * DataBuilder.SECTOR_SIZE;
      wrapper.setTime(time + i);
      List<byte[]> datas = wrapper.getData(split);
      for (byte[] data : datas) {
        try {
          storage.write(offset, data, 0, data.length);
        } catch (Exception e) {
          exitStatus = ExitStatus.EXCEPTION_EXIT;
          logger.error("fail to write, wrapper={}, write times={}", wrapper, writeTimes, e);
        }
        offset = offset + data.length;
      }
    }
  }

  /**
   * xx.
   */
  public static boolean canExit(int writeTimes) {
    if (System.currentTimeMillis() - startTime >= coordinatorToolConfiguration.getRumtimeMs()
        || writeTimes >= coordinatorToolConfiguration.getRoundTimes()) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * xx.
   */
  public static void startReadData() throws Exception {
    Random random = new Random();
    while (true) {
      DataWrapper wrapper = waitForReadingQueue.poll(1000L, TimeUnit.MILLISECONDS);
      if (exitStatus != ExitStatus.NOT_EXIT) {
        logger.warn("exit read thread");
        break;
      }

      if (wrapper == null) {
        // just wait for next read round.
        continue;
      }

      // read data to check randomly.
      int readTimes = random.nextInt(coordinatorToolConfiguration.getReadRepeatTimes()) + 1;
      try {
        readAndCheckData(wrapper, readTimes, random.nextBoolean());
        readCounter.incrementAndGet();
      } catch (Exception e) {
        logger.error("fail to read and check data wrapper=", wrapper, e);
        exitStatus = ExitStatus.EXCEPTION_EXIT;
      }
    }

    logger.warn("exit read thread, status={}", exitStatus);
  }

  private static void readAndCheckData(DataWrapper wrapper, int readTimes, boolean split) {
    for (int i = 0; i < readTimes; i++) {
      int interval = split ? 1 : (int) wrapper.getLengthSector();

      long offset = wrapper.getOffsetSector() * DataBuilder.SECTOR_SIZE;
      for (int j = 0; j < wrapper.getLengthSector(); j += interval) {
        byte[] buffer = new byte[DataBuilder.SECTOR_SIZE * interval];
        try {
          storage.read(offset + DataBuilder.SECTOR_SIZE * j, buffer, 0, buffer.length);
        } catch (Exception e) {
          logger.error("fail to read data wrapper={}", wrapper, e);
          exitStatus = ExitStatus.EXCEPTION_EXIT;
        }

        try {
          DataBuilder.canRebuild(buffer, wrapper.getOffsetSector() + j, wrapper.getTime());
        } catch (Exception e) {
          logger.error("fail to check data wrapper={}", wrapper, e);
          exitStatus = ExitStatus.MISMATCH_EXIT;
        }
      }
    }
  }

  /**
   * xx.
   */
  public static void generateWritePlan() throws Exception {
    // this is a fake generator
    PlansGenerator plansGenerator = new PlansGenerator(Long.MAX_VALUE, 0L, 0);
    int roundCount = 0;
    RoundFile roundFile = null;
    long expectIoTimes = 0;
    int generateRangesCount = 0;

    while (true) {
      // check if there is an error when reading, writing or checking
      if (exitStatus != ExitStatus.NOT_EXIT) {
        logger.warn("generate plan thread exit for status={}", exitStatus);
        roundFile.flush();
        break;
      }

      if (!plansGenerator.isOver()) {
        if (!syncGenerateWithWrite.tryAcquire(generateRangesCount, 1, TimeUnit.SECONDS)) {
          logger.warn("read counter={}, write counter={}, expected count={}", readCounter.get(),
              writeCounter.get(), expectIoTimes);
          continue;
        }

        if (roundFile != null) {
          roundFile.flush();
        }

        List<Range<Long>> ranges = plansGenerator.generate();
        if (ranges.isEmpty()) {
          logger.warn("it is strange to generate a empty ranges");
        }

        generateRangesCount = ranges.size();
        expectIoTimes += generateRangesCount;

        // every time we can not generate too many data wrappers, 
        // because of the limited jvm heap size. so there
        // is possible that generates times for every round.
        for (Range<Long> range : ranges) {
          DataWrapper dataWrapper = generateDataWrapper(range);
          roundFile.add(dataWrapper);
          waitForWritingQueue.add(dataWrapper);
        }
      }

      if (readCounter.get() == expectIoTimes && writeCounter.get() == expectIoTimes
          && plansGenerator.isOver()) {
        logger.warn("this round={} is over", roundCount);
        // flush written data to file
        if (roundFile != null) {
          logger.warn("flush this round data wrapper to file, roundFile={}", roundFile);
          roundFile.flush();
        }
      } else {
        logger.warn("read counter={}, write counter={}, expected count={}", readCounter.get(),
            writeCounter.get(), expectIoTimes);
        syncGenerateWithWrite.tryAcquire(generateRangesCount, 1, TimeUnit.SECONDS);
        continue;
      }

      // check can exit
      if (canExit(roundCount)) {
        exitStatus = ExitStatus.OK_EXIT;
        break;
      }

      logger.warn("start a new round={}, with snapshot={}", roundCount,
          coordinatorToolConfiguration.isSnapshot());
      if (roundCount > 0) {
        // wait for volume becoming stable
        VolumeUtils.waitForVolumeStable(volumeId, Long.MAX_VALUE);

        // start the previous data node which status is INC and wait for becoming OK.
        dataNodeManager.activatePreviousDataNodeAndWait();

        // kill some data node and reading all data to check its validate.
        dataNodeManager.deactivateDataNodeAndWait();

        logger.warn("sleep 1 minutes for volume to change status");
        Thread.sleep(60000);

        // wait for volume becoming available
        VolumeUtils.waitForVolumeAvailable(volumeId, Long.MAX_VALUE);

        Thread.sleep(600000);

      }

      plansGenerator = new PlansGenerator(coordinatorToolConfiguration.getVolumeSize(),
          coordinatorToolConfiguration.getDataByteSize(),
          coordinatorToolConfiguration.getMaxRangeCountPerGenerate());

      // reset the value
      writeCounter.set(0);
      readCounter.set(0);
      expectIoTimes = 0;
      generateRangesCount = 0;

      // generate a new round file
      roundFile = roundFileManager.generateNewRoundFile(snapshotId);
      roundFile.create();
      roundCount++;
      Thread.sleep(1000);
    }
    logger.warn("exit generate plan thread, status={}", exitStatus);
  }

  private static DataWrapper generateDataWrapper(Range<Long> range) {
    long offsetSector = range.lowerEndpoint() / DataBuilder.SECTOR_SIZE;
    long lengthSector = range.upperEndpoint() / DataBuilder.SECTOR_SIZE - offsetSector;
    return new DataWrapper(offsetSector, lengthSector, snapshotId, System.currentTimeMillis());
  }
}
