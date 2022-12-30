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
