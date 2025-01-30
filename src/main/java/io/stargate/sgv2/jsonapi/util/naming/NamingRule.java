package io.stargate.sgv2.jsonapi.util.naming;

/** Define a naming rule that can be applied to a string */
public interface NamingRule {
  /**
   * @return the name of the target that this rule is applied to
   */
  String name();

  /**
   * @return true if the input satisfies this naming rule, false otherwise
   */
  boolean apply(String input);
}
