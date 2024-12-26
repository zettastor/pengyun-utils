

package py.debug.service;

import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import py.common.PyService;
import py.debug.cmd.Cmd;
import py.debug.cmd.SetConfigCmd;
import py.debug.cmd.ShowConfigCmd;
import py.debug.cmd.SwitchProcessWriteMutationLogEnableOrDisableCmd;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class DataNodeAbstractServiceDebugger extends AbstractServiceDebugger {

  public DataNodeAbstractServiceDebugger(PyService pyService) {
    super(pyService);
  }

  @Override
  protected void addCmd(DebugConfigurator.Iface debugConfigurator,
      List<ImmutablePair<String, Cmd>> cmdList) {
    super.addCmd(debugConfigurator, cmdList);
    cmdList.add(new ImmutablePair<>(SHOW_CONFIG, new ShowConfigCmd(debugConfigurator)));
    cmdList.add(new ImmutablePair<>(SET_CONFIG, new SetConfigCmd(debugConfigurator)));
    cmdList.add(new ImmutablePair<>(SWITCH_ENABLE_WRITE_MUTATIONS,
        new SwitchProcessWriteMutationLogEnableOrDisableCmd(debugConfigurator)));
  }
}
