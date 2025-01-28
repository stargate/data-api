package io.stargate.sgv2.jsonapi.util;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * The CQL driver in particular stores values in maps of <string, string> where the values are
 * typed. So this class is helper methods to work with those maps for casting and defaulting values.
 */
public abstract class CqlOptionUtils {

  private CqlOptionUtils() {
    // prevent instantiation
  }

  /**
   * Gets the named key from the map is present and converts to the specified type using the
   * converter. You normally want to use one of the other getters that have type and defaults.
   *
   * @param map The map to get the value from
   * @param key The key to get the value for
   * @param converter The function to convert the string value to the desired type
   * @return The value of the key or null if not present, will throw an exception if the value is
   *     present and the converter throws an exception.
   * @param <T> The type to convert the value to.
   */
  public static <T> T getIfPresent(
      Map<String, String> map, String key, Function<String, T> converter) {
    return map.containsKey(key) ? converter.apply(map.get(key)) : null;
  }

  /**
   * Gets the named key from the map and converts it to a boolean if present.
   *
   * @param map The map to get the value from
   * @param key The key to get the value for
   * @return The boolean value or null if not present, will throw an exception if the value is
   *     present and {@link Boolean#valueOf(String)} throws an exception.
   */
  public static Boolean getBooleanIfPresent(Map<String, String> map, String key) {
    return getIfPresent(map, key, Boolean::valueOf);
  }

  /**
   * Gets the named key from the map and converts it to a boolean if present, otherwise returns the
   * default value.
   *
   * @param map The map to get the value from
   * @param key The key to get the value for
   * @param defaultValue The default value to return if the key is not present
   * @return The valuw of the key or the default value if the key is not present, will throw an
   *     exception if the value is present and {@link Boolean#parseBoolean(String)} throws an
   *     exception.
   */
  public static boolean getOrDefault(Map<String, String> map, String key, boolean defaultValue) {
    // assume if you have a default you want value type
    return map.containsKey(key) ? Boolean.parseBoolean(map.get(key)) : defaultValue;
  }

  /**
   * Puts the value in the map if it is not null, otherwise puts the default value.
   *
   * @param map The map to put the value in
   * @param key The key to put the value under
   * @param value The value to put in the map, may be null
   * @param defaultValue The default value to put in the map if the value is null
   * @return Previous value in the map or null if not present
   */
  public static String putOrDefault(
      Map<String, String> map, String key, Boolean value, boolean defaultValue) {
    return map.put(key, value == null ? Boolean.toString(defaultValue) : value.toString());
  }

  /**
   * Puts the value in the map
   *
   * @param map The map to put the value in
   * @param key The key to put the value under
   * @param value The value to put in the map, must not be null
   * @return Previous value in the map or null if not present
   */
  public static String put(Map<String, String> map, String key, Boolean value) {
    Objects.requireNonNull(value, "value must not be null");
    return map.put(key, value.toString());
  }
}
