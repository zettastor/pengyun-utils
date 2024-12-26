

package py.utils.dd.client;

import py.DeploymentDaemonClientFactory;

/**
 * deploymentdaemon client main
 *
 * <p>you can through dd.sh to execute put, activate, getstatus, deactivate at command line.
 *
 * <p>DeploymentDaemon receives command from command line and then call DeploymentDaemonClient.
 */
public class DeploymentDaemon {

  /**
   * xx.
   */
  public static void main(String[] args) {
    DeploymentDaemonClientFactory deploymentDaemonClientFactory = null;
    try {
      deploymentDaemonClientFactory = new DeploymentDaemonClientFactory(1);
      DeploymentDaemonClientHandler ddClient = new DeploymentDaemonClientHandler();
      ddClient.setDeploymentDaemonClientFactory(deploymentDaemonClientFactory);
      ddClient.deactivate();
    } finally {
      if (deploymentDaemonClientFactory != null) {
        deploymentDaemonClientFactory.close();
      }
    }
  }

}
