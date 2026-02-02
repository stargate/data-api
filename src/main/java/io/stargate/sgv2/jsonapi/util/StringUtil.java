package io.stargate.sgv2.jsonapi.util;

import java.util.Optional;

public class StringUtil {

  private StringUtil() {}

  public static String normalizeOptionalString(String string) {
    return string == null || string.isBlank() ? "" : string;
  }

  public static String normalizeOptionalString(Optional<String> string) {
    return normalizeOptionalString(string.orElse(""));
  }
}
