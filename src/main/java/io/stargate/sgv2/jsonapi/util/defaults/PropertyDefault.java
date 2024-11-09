package io.stargate.sgv2.jsonapi.util.defaults;

import java.util.Map;

public abstract class PropertyDefault<KeyT, ValueT> {

  protected final KeyT key;
  protected final Default<ValueT> defaultValue;

  PropertyDefault(KeyT key, Default<ValueT> defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  public KeyT key() {
    return key;
  }

  public boolean isPresent(ValueT value) {
    return defaultValue.isPresent(value);
  }

  public ValueT defaultValue() {
    return defaultValue.defaultValue();
  }

  /**
   * Test if the value is present in the map, according to {@link Default#isPresent(Object)}
   *
   * @param map
   * @return
   */
  public boolean isPresent(Map<KeyT, ValueT> map) {
    return defaultValue.isPresent(map.get(key));
  }

  public ValueT getWithDefault(Map<KeyT, ValueT> map) {
    return defaultValue.apply(map.get((key)));
  }

  /**
   * Read the key from the map and use {@link Default#isPresent(Object)} to determine if the value
   * is present, if it is missing then apply the default.
   *
   * @param map
   */
  public ValueT putOrDefault(Map<KeyT, ValueT> map, ValueT value) {
    return map.put(key, defaultValue.apply(value));
  }

  public abstract static class StringablePropertyDefault<KeyT, ValueT>
      extends PropertyDefault<KeyT, ValueT> {

    private final Default.StringableDefault<ValueT> defaultStringable;

    protected StringablePropertyDefault(KeyT key, Default.StringableDefault<ValueT> defaultValue) {
      super(key, defaultValue);
      this.defaultStringable = defaultValue;
    }

    public void putOrDefaultStringable(Map<KeyT, String> map, ValueT value) {
      map.put(key, defaultValue.apply(value).toString());
    }

    public ValueT getWithDefaultStringable(Map<KeyT, String> map) {
      return defaultStringable.apply(map.get((key)));
    }
  }
}
