package io.stargate.sgv2.jsonapi.service.schema.naming;

/** Define a naming rule that can be applied to a string */
public abstract class NamingRule {
  /** The name of thing being validated */
  private final String name;

  public NamingRule(String name) {
    this.name = name;
  }

  /**
   * @return the name of the target that this rule is applied to
   */
  public String name() {
    return name;
  }
}
