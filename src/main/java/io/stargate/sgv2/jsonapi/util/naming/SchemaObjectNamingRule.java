package io.stargate.sgv2.jsonapi.util.naming;

import java.util.regex.Pattern;

public abstract class SchemaObjectNamingRule implements NamingRule {

  public static final int MAX_NAME_LENGTH = 48;
  public static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
  private final String name;

  public SchemaObjectNamingRule(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }
  ;

  @Override
  public boolean apply(String name) {
    return name != null
        && !name.isEmpty()
        && name.length() <= MAX_NAME_LENGTH
        && PATTERN_WORD_CHARS.matcher(name).matches();
  }
}
