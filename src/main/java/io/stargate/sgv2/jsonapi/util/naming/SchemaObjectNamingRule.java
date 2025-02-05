package io.stargate.sgv2.jsonapi.util.naming;

import java.util.regex.Pattern;

/**
 * An abstract implementation of the {@link NamingRule} interface that provides a default naming
 * rule for schema objects (keyspaces, tables, collections, indexes).
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
public abstract class SchemaObjectNamingRule implements NamingRule {

  private static final int MAX_NAME_LENGTH = 48;
  private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
  private final String name;

  public SchemaObjectNamingRule(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public int getMaxLength() {
    return MAX_NAME_LENGTH;
  }

  @Override
  public boolean apply(String name) {
    return name != null
        && !name.isEmpty()
        && name.length() <= MAX_NAME_LENGTH
        && PATTERN_WORD_CHARS.matcher(name).matches();
  }
}
