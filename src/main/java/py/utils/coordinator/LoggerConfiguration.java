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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

/**
 * xx.
 */
public class LoggerConfiguration {

  private String fileName = "logs/coordinator_tool.log";
  private Level level;

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Level getLevel() {
    return level;
  }

  /**
   * xx.
   */
  public void setLevel(String level) {
    if (0 == level.compareToIgnoreCase("debug")) {
      this.level = Level.DEBUG;
    } else if (0 == level.compareToIgnoreCase("info")) {
      this.level = Level.INFO;
    } else if (0 == level.compareToIgnoreCase("warn")) {
      this.level = Level.WARN;
    } else if (0 == level.compareToIgnoreCase("error")) {
      this.level = Level.ERROR;
    }
  }

  public void setLevel(Level level) {
    this.level = level;
  }

  /**
   * xx.
   */
  public void init() {
    PatternLayout layout = new PatternLayout();
    String conversionPattern = "%-5p[%d][%t]%C(%L):%m%n";
    layout.setConversionPattern(conversionPattern);

    // creates console appender
    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setLayout(layout);
    consoleAppender.setThreshold(Level.WARN);
    consoleAppender.setTarget("System.out");
    consoleAppender.setEncoding("UTF-8");
    consoleAppender.activateOptions();

    RollingFileAppender rollingFileAppender = new RollingFileAppender();
    rollingFileAppender.setFile(fileName);
    rollingFileAppender.setLayout(layout);
    rollingFileAppender.setThreshold(level);
    rollingFileAppender.setMaxBackupIndex(10);
    rollingFileAppender.setMaxFileSize("400MB");
    rollingFileAppender.activateOptions();

    // configures the root logger
    Logger rootLogger = Logger.getRootLogger();
    rootLogger.setLevel(level);
    // configures the root logger
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(consoleAppender);
    rootLogger.addAppender(rollingFileAppender);

    Logger logger1 = Logger.getLogger("org.hibernate");
    logger1.setLevel(Level.WARN);

    Logger logger2 = Logger.getLogger("org.springframework");
    logger2.setLevel(Level.WARN);

    Logger logger3 = Logger.getLogger("com.opensymphony");
    logger3.setLevel(Level.WARN);

    Logger logger4 = Logger.getLogger("org.apache");
    logger4.setLevel(Level.WARN);

    Logger logger5 = Logger.getLogger("com.googlecode");
    logger5.setLevel(Level.WARN);

    Logger logger6 = Logger.getLogger("com.twitter.common.stats");
    logger6.setLevel(Level.WARN);

    Logger logger7 = Logger.getLogger("com.mchange");
    logger7.setLevel(Level.WARN);
  }
}
