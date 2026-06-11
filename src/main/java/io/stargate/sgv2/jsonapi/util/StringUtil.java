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

  public static boolean isNullOrBlank(String string) {
    return string == null || string.isBlank();
  }

  /**
   * Returns {@code value} unchanged if it is non-null and not blank; otherwise throws {@link
   * IllegalArgumentException} naming the offending {@code name}.
   */
  public static String requireNonBlank(String value, String name) {
    if (isNullOrBlank(value)){
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return value;
  }
}
