package io.stargate.sgv2.jsonapi.service.schema.naming;

public class FieldNamingRule extends NamingRule {
  public FieldNamingRule() {
    super("Field");
  }

  /**
   * Validates the given Field name. The only limit is that the name must NOT start with a dollar
   * sign ($). Note that field names are hierarchical, but validation is done on per-segment basis.
   *
   * @param depth Level of the field in the hierarchy (1 for root, 2 for first child, etc.)
   * @param name the name to validate
   * @return true if the name is valid, false otherwise
   */
  public boolean apply(int depth, String name) {
    // Dollar not allowed for any fields (not just root)
    return !name.startsWith("$");
  }
}
