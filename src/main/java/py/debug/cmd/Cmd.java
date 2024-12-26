
package py.debug.cmd;

/**
 * xx.
 */
public interface Cmd {

  /**
   * send rpc request, such as setLogLevel, showConfig ...
   */
  void doCmd(String[] args);

  /**
   * show the usage description of current cmd.
   */
  void usage();

}
