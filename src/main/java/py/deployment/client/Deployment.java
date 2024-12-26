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

package py.deployment.client;

import java.util.List;
import java.util.Map;
import py.dd.common.ServiceMetadata;
import py.deployment.common.DeploymentConfiguration;
import py.deployment.common.DeploymentOperation;

/**
 * A interface includes all operation handler.
 */
public interface Deployment {

  /**
   *xx.
   */
  public void init(String serviceName, int servicePort);


  /**
   * Transfer package of service to remote machine for deployment.
   *
   * @return true  the operation is done successfully. false: something wrong when handle the
   *        operation.
   */
  public boolean transfer();

  /**
   * Deploy service to remote machine. This operation is done with "transfer", "activate"
   * operations.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *        operation.
   */
  public boolean deploy();

  /**
   * Upgrade service on remote machine. This operation is done with "deactivate", "deploy"
   * operations.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *        operation.
   */
  public boolean upgrade();

  /**
   * Wipeout service on remote machine. This operation will destroy all service, and then clear all
   * relative resources of the running service.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *        operation.
   */
  public boolean wipeout();


  public boolean wipeout(DeploymentConfiguration config);

  /**
   * Force to stop running service on remote machine.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *        operation.
   */
  public boolean destroy();

  /**
   * Tar the package of the service, link service to running path and then start the service.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *       operation.
   */
  public boolean activate();

  /**
   * Stop the running service after the service is ready to stop. Service can do rest things after
   * receive the request.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *      operation.
   */
  public boolean deactivate();

  /**
   * Start the service basing on current running path.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *      operation.
   */
  public boolean start();

  /**
   * Restart the service basing on current running path.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *      operation.
   */
  public boolean restart();

  /**
   * Configure remote service basing on config file.
   *
   * @return true: the operation is done successfully. false: something wrong when handle the
   *      operation.
   */
  public boolean configure();

  /**
   * Backup the license decrypt key in the old package.
   */
  public boolean backupKey();

  /**
   * Reuse the license decrypt key into the new package.
   */
  public boolean useBackupKey();

  /**
   * Check status of service on remote machine. map: the operation is done successfully. null:
   * something wrong when handle the operation.
   */
  public Map<String, ServiceMetadata> status();

  /**
   * xx.
   */
  public Map<DeploymentOperation, List<String>> getFailures();

  /**
   * After package of some service was transfered to target host, it is necessary to prepare
   * workspace for the service.
   */
  public boolean prepareWorkspace();
}
