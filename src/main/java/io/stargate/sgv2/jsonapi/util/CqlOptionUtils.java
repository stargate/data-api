package io.stargate.sgv2.jsonapi.util;

import java.util.Map;
import java.util.function.Function;

/**
 * The CQL driver in particular stores values in maps of <string, string> where the values are
 * typed. So this class is helper methods to work with those maps for casting and defaulting values.
 */
public abstract class CqlOptionUtils {

  public static Boolean getBooleanIfPresent(Map<String, String> map, String key) {
    return getIfPresent(map, key, Boolean::valueOf);
  }

  public static boolean getOrDefault(Map<String, String> map, String key, boolean defaultValue) {
    // assume if you have a default you want value type
    return map.containsKey(key) ? Boolean.parseBoolean(map.get(key)) : defaultValue;
  }

  public static String putOrDefault(
      Map<String, String> map, String key, Boolean value, boolean defaultValue) {
    return map.put(key, value == null ? Boolean.toString(defaultValue) : value.toString());
  }

  public static <T> T getIfPresent(
      Map<String, String> map, String key, Function<String, T> converter) {
    return map.containsKey(key) ? converter.apply(map.get(key)) : null;
  }
}
