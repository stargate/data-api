package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;

/** Define a naming rule that can be applied to a string */
public abstract class NamingRule {
  private final String name;
  private final SchemaObject.SchemaObjectType schemaType;

  public NamingRule(SchemaObject.SchemaObjectType schemaType, String name) {
    this.schemaType = schemaType;
    this.name = name;
  }

  /**
   * @return the type of schema object that this rule is applied to
   */
  public SchemaObject.SchemaObjectType schemaType() {
    return schemaType;
  }

  /**
   * @return the name of the target that this rule is applied to
   */
  public String name() {
    return name;
  }
  ;

  /**
   * @return the maximum allowed length for the name.
   */
  public abstract int getMaxLength();

  /**
   * @return true if the input satisfies this naming rule, false otherwise
   */
  public abstract boolean apply(String input);
}
