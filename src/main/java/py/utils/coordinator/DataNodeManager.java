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

package py.utils.coordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.PyService;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceStatus;

/**
 * xx.
 */
public class DataNodeManager {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeManager.class);

  private final List<Instance> dataNodes;
  private Instance destroyInstance;

  /**
   * xx.
   */
  public DataNodeManager() throws GenericThriftClientFactoryException, TException {
    dataNodes = new ArrayList<Instance>();
    for (Instance instance : VolumeUtils.getServiceInstance(PyService.DATANODE.getServiceName())) {
      dataNodes.add(instance);
    }
  }

  /**
   * xx.
   */
  public void confirmAllDataNodeOk() throws Exception {
    Set<Instance> instances = VolumeUtils.getServiceInstance(PyService.DATANODE.getServiceName(),
        InstanceStatus.SICK);
    for (Instance instance : instances) {
      logger.warn("active datanode={} when starting coordinator tool", instance);
      VolumeUtils.active(instance, Long.MAX_VALUE);
    }

    for (Instance instance : instances) {
      logger.warn("wait datanode={} to become OK", instance);
      VolumeUtils.waitForServiceToSpecifiedStatus(instance, InstanceStatus.HEALTHY, Long.MAX_VALUE);
    }
  }

  /**
   * xx.
   */
  public void deactivateDataNodeAndWait() throws Exception {
    // select a data node to deactivate
    Random random = new Random();
    Instance instance = null;
    while (true) {
      instance = dataNodes.get(random.nextInt(dataNodes.size()));
      if (destroyInstance != null) {
        if (instance.getId().getId() == destroyInstance.getId().getId()) {
          continue;
        }
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        logger.error("", e);
      }

      VolumeUtils.deactive(instance, Long.MAX_VALUE);
      VolumeUtils.waitForServiceToSpecifiedStatus(instance, InstanceStatus.SICK, Long.MAX_VALUE);

      destroyInstance = instance;
      logger.warn("deactive instance={}", instance);
      break;
    }

  }

  /**
   * xx.
   */
  public void activatePreviousDataNodeAndWait() throws Exception {
    if (destroyInstance == null) {
      logger.warn("there is no shutdown datanode, all datanode={}", dataNodes);
      return;
    }

    VolumeUtils.active(destroyInstance, Long.MAX_VALUE);
    VolumeUtils.waitForServiceToSpecifiedStatus(
        destroyInstance, InstanceStatus.HEALTHY, Long.MAX_VALUE);
    logger.warn("active instance={}", destroyInstance);
  }
}
