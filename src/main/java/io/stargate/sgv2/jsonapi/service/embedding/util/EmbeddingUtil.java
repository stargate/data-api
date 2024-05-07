package io.stargate.sgv2.jsonapi.service.embedding.util;

import java.util.Map;

public class EmbeddingUtil {
  /**
   * Helper method to replace parameters in a template string with values from a map: placeholders
   * are of form {@code {parameterName}} and matching value to look for in the map is String {@code
   * "parameterName"}.
   *
   * @param template Template with placeholders to replace
   * @param parameters Parameters to replace in the template
   * @return Processed template with replaced parameters
   */
  public static String replaceParameters(String template, Map<String, Object> parameters) {
    final int len = template.length();
    StringBuilder baseUrl = new StringBuilder(len);

    for (int i = 0; i < len; ) {
      char c = template.charAt(i++);
      int end;

      if ((c != '{') || (end = template.indexOf('}', i)) < 0) {
        baseUrl.append(c);
        continue;
      }
      String key = template.substring(i, end);
      i = end + 1;

      Object value = parameters.get(key);
      if (value == null) {
        throw new IllegalArgumentException(
            "Missing URL parameter '" + key + "' (available: " + parameters.keySet() + ")");
      }
      baseUrl.append(value);
    }
    return baseUrl.toString();
  }

  /**
   * Helper method that has logic wrt whether OpenAI (azure or regular) accepts {@code "dimensions"}
   * parameter or not.
   *
   * @param modelName OpenAI model to check
   * @return True if given OpenAI model accepts (and expects} {@code "dimensions"} parameter; false
   *     if not.
   */
  public static boolean acceptsOpenAIDimensions(String modelName) {
    return !modelName.endsWith("-ada-002");
  }
}
