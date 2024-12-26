

package py.debug.cmd;

import java.util.Map;
import py.debug.DebuggerOutputImpl;
import py.debug.DynamicParamConfig;
import py.thrift.share.DebugConfigurator;

/**
 * xx.
 */
public class ShowLogLevel extends ShowConfigCmd {

  public ShowLogLevel(DebugConfigurator.Iface debugConfigurator) {
    super(debugConfigurator);
  }

  @Override
  protected void print(String[] args, Map<String, String> configurations) {
    for (Map.Entry<String, String> value : configurations.entrySet()) {
      if (value.getKey().contains(DynamicParamConfig.LOG_LEVEL)) {
        DebuggerOutputImpl.getInstance().print(value.getKey() + "=" + value.getValue() + "\n");
      }
    }
  }
}
