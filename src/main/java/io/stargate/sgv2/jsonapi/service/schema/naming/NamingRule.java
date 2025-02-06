package io.stargate.sgv2.jsonapi.service.schema.naming;

/** Define a naming rule that can be applied to a string */
public interface NamingRule {
  /**
   * @return the name of the target that this rule is applied to
   */
  String name();

  /**
   * @return the maximum allowed length for the name.
   */
  int getMaxLength();

  /**
   * @return true if the input satisfies this naming rule, false otherwise
   */
  boolean apply(String input);
}
