package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.Map;

/**
 * A default for a property in a map
 *
 * @param <KeyT>
 * @param <ValueT>
 */
public abstract class Property<KeyT, ValueT> {

  protected final KeyT key;
  protected final Default<ValueT> defaultValue;

  Property(KeyT key, Default<ValueT> defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  /** Get the key for the property. */
  public KeyT key() {
    return key;
  }

  /**
   * Test if the value is present using {@link Default#isPresent(Object)}
   *
   * @param value
   * @return
   */
  public boolean isPresent(ValueT value) {
    return defaultValue.isPresent(value);
  }

  /**
   * Test if the value is present in the map
   *
   * @param map
   * @return
   */
  public boolean isPresent(Map<KeyT, ValueT> map) {
    return isPresent(map.get(key));
  }

  /**
   * Get the default value
   *
   * @return
   */
  public ValueT defaultValue() {
    return defaultValue.defaultValue();
  }

  /**
   * Read the key from the map, returns the value or the default
   *
   * @param map
   * @return
   */
  public ValueT getWithDefault(Map<KeyT, ValueT> map) {
    try {
      return defaultValue.apply(map.get((key)));
    } catch (RequiredValue.RequiredValueMissingException e) {
      throw new RequiredValue.RequiredValueMissingException(
          "Required value missing for key: " + key);
    }
  }

  /**
   * Read the key from the map, returns the value if the {@link #isPresent(Object)} is true,
   * otherwise <code>null</code>.
   *
   * <p>This <em>bypasses</em> the default value.
   *
   * @param map The map to read from
   * @return The value if it is present, otherwise <code>null</code>
   */
  public ValueT getIfPresent(Map<KeyT, ValueT> map) {
    try {
      var value = map.get(key);
      return isPresent(value) ? value : null;
    } catch (RequiredValue.RequiredValueMissingException e) {
      throw new RequiredValue.RequiredValueMissingException(
          "Required value missing for key: " + key);
    }
  }

  /**
   * Write the value or the default value to the map , return any previous value
   *
   * @param map
   * @param value
   * @return
   */
  public ValueT putOrDefault(Map<KeyT, ValueT> map, ValueT value) {
    return map.put(key, defaultValue.apply(value));
  }

  public static class Stringable<StringableKeyT, StringableValueT>
      extends Property<StringableKeyT, StringableValueT> {

    protected final Default.Stringable<StringableValueT> stringableDefaultValue;

    public Stringable(StringableKeyT key, Default.Stringable<StringableValueT> defaultValue) {
      super(key, defaultValue);
      this.stringableDefaultValue = defaultValue;
      ;
    }

    public StringableValueT getWithDefaultStringable(Map<StringableKeyT, String> map) {
      return stringableDefaultValue.applyToType(map.get((key)));
    }

    /**
     * Read the key from the map, returns the value if the {@link Default#isPresent()} is true,
     * otherwise <code>null</code>.
     *
     * <p>This <em>bypasses</em> the default value.
     *
     * @param map The map to read from
     * @return The value if it is present, otherwise <code>null</code>
     */
    public StringableValueT getIfPresentStringable(Map<StringableKeyT, String> map) {
      try {
        var value = map.get(key);
        return stringableDefaultValue.isPresent(value)
            ? stringableDefaultValue.applyToType(value)
            : null;
      } catch (RequiredValue.RequiredValueMissingException e) {
        throw new RequiredValue.RequiredValueMissingException(
            "Required value missing for key: " + key);
      }
    }

    public String putOrDefaultStringable(Map<StringableKeyT, String> map, StringableValueT value) {
      return map.put(key, stringableDefaultValue.applyToString(value));
    }
  }
}
