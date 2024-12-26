
package py.utils.test;

import py.common.struct.EndPoint;
import py.dih.client.DihClientFactory;
import py.dih.client.DihServiceBlockingClientWrapper;

/**
 * xx.
 */
public class ShutdownTest {

  /**
   * xx.
   */
  public static void main(String[] args) {
    DihClientFactory dihClientFactory = null;
    try {
      dihClientFactory = new DihClientFactory(1);
      EndPoint endPoint = new EndPoint("10.0.1.112", 10000);
      DihServiceBlockingClientWrapper client = dihClientFactory.build(endPoint, 20000);

      client.getDelegate().shutdown();
    } catch (Exception e) {
      System.out.println("exception occur");
    } finally {
      if (dihClientFactory != null) {
        dihClientFactory.close();
      }
    }
  }

}
