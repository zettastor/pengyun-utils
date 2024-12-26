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

package py.app;

import java.util.Arrays;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * xx.
 */
@Configuration
public class Config {

  @Value("${dih:}")
  String dih;
  @Value("${ssdOnly:false}")
  boolean ssdOnly;
  @Value("${volumeSizeMB:1024}")
  long volumeSizeMb;
  @Value("${volumeID:}")
  long volumeId;
  @Value("${driverHosts:NONE}")
  String[] driverHosts;
  @Value("${availableDataNodes:3}")
  int availableDataNodes;
  @Value("${minArchiveCount:1}")
  int minArchiveCount;
  @Value("${waitForVolumeStable:true}")
  boolean waitForVolumeStable; // false for available
  @Value("${pydName:}")
  String pydName;
  @Value("${debug:false}")
  boolean debug;
  @Value("${simpleConfig:false}")
  boolean simpleConfig;

  @Override
  public String toString() {
    return "Config{" + "dih='" + dih + '\'' + ", ssdOnly=" + ssdOnly + ", volumeSizeMB="
        + volumeSizeMb
        + ", volumeID=" + volumeId + ", driverHosts=" + Arrays.toString(driverHosts)
        + ", availableDataNodes="
        + availableDataNodes + ", minArchiveCount=" + minArchiveCount + ", waitForVolumeStable="
        + waitForVolumeStable + ", pydName='" + pydName + '\'' + ", debug=" + debug
        + ", simpleConfig="
        + simpleConfig + '}';
  }
}
