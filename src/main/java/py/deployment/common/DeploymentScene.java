
package py.deployment.common;

/**
 * xx.
 */
public enum DeploymentScene {
  INTERNAL("internal"), RELEASE("release"), OPENSOURCE("opensource");

  private String value;

  private DeploymentScene(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
