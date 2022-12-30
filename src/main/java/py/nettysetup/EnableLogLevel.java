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

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;

/**
 * xx.
 */
public class EnableLogLevel {

  /**
   * xx.
   */
  public static void enable(Level level) {
    PatternLayout layout = new PatternLayout();
    String conversionPattern = "%-5p[%d][%t]%C(%L):%m%n";
    layout.setConversionPattern(conversionPattern);

    // creates console appender
    ConsoleAppender consoleAppender = new ConsoleAppender();
    consoleAppender.setLayout(layout);
    consoleAppender.setTarget("System.out");
    consoleAppender.setEncoding("UTF-8");
    consoleAppender.activateOptions();

    // configures the root logger
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    rootLogger.setLevel(level);
    rootLogger.removeAllAppenders();
    rootLogger.addAppender(consoleAppender);
  }
}
