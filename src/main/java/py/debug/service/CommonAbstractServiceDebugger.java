

package py.debug.service;

import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.common.PyService;
import py.debug.cmd.Cmd;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class CommonAbstractServiceDebugger extends AbstractServiceDebugger {

  public CommonAbstractServiceDebugger(PyService pyService) {
    super(pyService);
  }

  @Override
  protected void addCmd(DebugConfigurator.Iface debugConfigurator,
      List<ImmutablePair<String, Cmd>> cmdList) {
    super.addCmd(debugConfigurator, cmdList);
  }
}
