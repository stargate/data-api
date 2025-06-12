package io.stargate.sgv2.jsonapi.service.schema.naming;

/**
 * {@link NamingRule} for validating field names: names that come from properties of JSON document
 * used to create a Document in a Collection. These names form a hierarchy of fields, where each
 * field can have sub-fields.
 *
 * <p>The rules for field names are:
 *
 * <ul>
 *   <li>Field names must not be Empty (length > 0)
 *   <li>Field names must not start with a dollar sign ($).
 *   <li>Field names are hierarchical (forming a Path), but validation is done on segment-by-segment
 *       (individual JSON property name) basis.
 * </ul>
 */
public class FieldNamingRule extends NamingRule {
  public FieldNamingRule() {
    super("Field");
  }

  /**
   * Validates the given Field name. The only limit is that the name must NOT start with a dollar
   * sign ($). Note that field names are hierarchical, but validation is done on name-by-name basis.
   *
   * @param name the name to validate
   * @return true if the name is valid, false otherwise
   */
  public boolean apply(String name) {
    // Dollar not allowed to start any field name (not just root); empty names are also invalid
    return !name.isBlank() && !name.startsWith("$");
  }
}
