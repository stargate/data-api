package io.stargate.sgv2.jsonapi.service.schema.naming;

import io.stargate.sgv2.jsonapi.exception.ErrorTemplate;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;
import java.util.Map;
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
  private final SchemaObjectType schemaType;

  public SchemaObjectNamingRule(SchemaObjectType schemaType) {
    super(schemaType.name());
    this.schemaType = schemaType;
  }

  /**
   * @return the type of schema object that this rule is applied to
   */
  public SchemaObjectType schemaType() {
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

  /**
   * Validate the name against the naming rule, and throw a {@link SchemaException} if the name is
   * invalid.
   *
   * @param name The name to validate.
   * @return The validated name, same as the rule passed in.
   * @throws SchemaException with {@link SchemaException.Code#UNSUPPORTED_SCHEMA_NAME} if the name
   *     is invalid.
   */
  public String checkRule(String name) {

    if (!apply(name)) {
      throw SchemaException.Code.UNSUPPORTED_SCHEMA_NAME.get(
          Map.of(
              "schemaType",
              schemaType().apiName(),
              "maxNameLength",
              String.valueOf(getMaxLength()),
              "unsupportedSchemaName",
              ErrorTemplate.replaceIfNull(name)));
    }
    return name;
  }
}
