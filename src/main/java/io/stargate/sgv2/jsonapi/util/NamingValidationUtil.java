package io.stargate.sgv2.jsonapi.util;

import java.util.regex.Pattern;

public interface NamingValidationUtil {

  String KEYSPACE_SCHEMA_NAME = "keyspace";
  String COLLECTION_SCHEMA_NAME = "collection";
  String TABLE_SCHEMA_NAME = "table";
  String INDEX_SCHEMA_NAME = "index";

  String NULL_SCHEMA_NAME = "null";

  int NAME_LENGTH = 48;
  Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");

  static boolean isValidName(String name) {
    return name != null
        && !name.isBlank()
        && name.length() <= NAME_LENGTH
        && PATTERN_WORD_CHARS.matcher(name).matches();
  }
}
