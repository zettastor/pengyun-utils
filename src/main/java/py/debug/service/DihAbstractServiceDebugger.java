

package py.debug.service;

import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.common.PyService;
import py.debug.cmd.Cmd;
import py.debug.cmd.SetDataNodeConfigurationCmd;
import py.debug.cmd.ShowServiceCmd;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class DihAbstractServiceDebugger extends AbstractServiceDebugger {

  public DihAbstractServiceDebugger(PyService pyService) {
    super(pyService);
  }

  @Override
  protected void addCmd(DebugConfigurator.Iface debugConfigurator,
      List<ImmutablePair<String, Cmd>> cmdList) {
    super.addCmd(debugConfigurator, cmdList);
    cmdList.add(new ImmutablePair<>(SHOW_SERVICE, new ShowServiceCmd(debugConfigurator)));
    cmdList.add(new ImmutablePair<>(SET_CONFIG_OF_DATANODE,
        new SetDataNodeConfigurationCmd(debugConfigurator)));
  }
}
