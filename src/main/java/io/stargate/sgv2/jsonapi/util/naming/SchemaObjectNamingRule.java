package io.stargate.sgv2.jsonapi.util.naming;

import java.util.regex.Pattern;

public abstract class SchemaObjectNamingRule implements NamingRule {

  public static final int NAME_LENGTH = 48;
  public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");

  @Override
  public abstract String name();

  @Override
  public boolean apply(String name) {
    return name != null
        && !name.isBlank()
        && name.length() <= NAME_LENGTH
        && PATTERN_WORD_CHARS.matcher(name).matches();
  }
}
