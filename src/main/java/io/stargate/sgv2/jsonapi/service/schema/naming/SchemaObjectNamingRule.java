package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.regex.Pattern;

/**
 * An abstract class of the {@link NamingRule} that provides a default naming rule for schema
 * objects (keyspaces, tables, collections, indexes).
 *
 * <p>This class validates a given name based on the following criteria:
 *
 * <ul>
 *   <li>The name must not be or empty.
 *   <li>The length of the name must not exceed {@value #MAX_NAME_LENGTH} characters.
 *   <li>The name must consist solely of word characters as defined by the regular expression {@link
 *       #PATTERN_WORD_CHARS}. Word characters typically include letters, digits, and underscores.
 * </ul>
 */
public abstract class SchemaObjectNamingRule extends NamingRule {

  private static final int MAX_NAME_LENGTH = 48;
  private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
  private final SchemaObject.SchemaObjectType schemaType;

  public SchemaObjectNamingRule(SchemaObject.SchemaObjectType schemaType) {
    super(schemaType.name());
    this.schemaType = schemaType;
  }

  /**
   * @return the type of schema object that this rule is applied to
   */
  public SchemaObject.SchemaObjectType schemaType() {
    return schemaType;
  }

  /**
   * Validates the given name. The name must not be null or empty, must not exceed {@value
   * #MAX_NAME_LENGTH} characters, and must not contain non-alphanumeric-underscore characters
   *
   * @param name the name to validate
   * @return true if the name is valid, false otherwise
   */
  public boolean apply(String name) {
    return name != null
        && !name.isEmpty()
        && name.length() <= getMaxLength()
        && PATTERN_WORD_CHARS.matcher(name).matches();
  }

  /**
   * @return the maximum allowed length of a name
   */
  public int getMaxLength() {
    return MAX_NAME_LENGTH;
  }
}
