
package py.debug.cmd;

/**
 * xx.
 */
public class ExitCmd implements Cmd {

  @Override
  public void doCmd(String[] args) {
    CmdManager.getInstance().setExitDebug(true);
  }

  @Override
  public void usage() {

  }

}
