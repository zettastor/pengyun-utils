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

package py.app;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * xx.
 */
@Configuration
public class SnapshotConfig {

  @Value("${dih:}")
  String dih;

  @Value("${volumeId:}")
  long volumeId;

  @Value("${snapshotCount:}")
  int snapshotCount;

  @Value("${snapshotName:tyr_snapshot}")
  String snapshotName;

  @Value("${snapshotDescription:description}")
  String snapshotDescription;

  @Value("${createSnapshotIntervalSeconds:10}")
  int createSnapshotIntervalSeconds;

  @Value("${debug:false}")
  boolean debug;

  @Override
  public String toString() {
    return "SnapshotConfig{"
        + "dih='" + dih + '\''
        + ", volumeId=" + volumeId
        + ", snapshotCount=" + snapshotCount
        + ", snapshotName='" + snapshotName + '\''
        + ", snapshotDescription='" + snapshotDescription + '\''
        + ", createSnapshotIntervalSeconds=" + createSnapshotIntervalSeconds
        + '}';
  }
}
