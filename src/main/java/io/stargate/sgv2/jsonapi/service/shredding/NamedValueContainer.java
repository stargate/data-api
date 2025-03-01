package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.util.recordable.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * Base implementation for a {@link NamedValueContainer} that maintains the order the named values
 * were added.
 *
 * <p>Marked abstract to force the instantiation of a concrete types the define the {@link
 * NamedValue} type.
 *
 * <p>Provides some helper methods and supports {@link PrettyPrintable} to make it easier to debug.
 *
 * @param <NameT> The type of the name, this is the key in the map
 * @param <ValueT> The type of the value stored in the {@link NamedValue}
 * @param <NvT> The type of the {@link NamedValue} stored in the map
 */
public abstract class NamedValueContainer<NameT, ValueT, NvT extends NamedValue<NameT, ValueT, ?>>
    extends LinkedHashMap<NameT, NvT> implements Recordable {

  public NamedValueContainer() {
    super();
  }

  public NamedValueContainer(int initialCapacity) {
    super(initialCapacity);
  }

  public NamedValueContainer(NamedValueContainer<NameT, ValueT, NvT> container) {
    super(container);
  }

  public NamedValueContainer(Collection<NvT> values) {
    super();
    putAll(values);
  }

  /**
   * Helper to add a {@link NamedValue} to the container, keyed on the {@link NamedValue#name()}.
   *
   * @param namedValue The {@link NamedValue} to add
   * @return The previous value associated with the name, or null if there was no mapping for the
   *     name
   */
  public NamedValue<NameT, ValueT, ?> put(NvT namedValue) {
    return put(namedValue.name(), namedValue);
  }

  public void putAll(Collection<NvT> namedValues) {
    Objects.requireNonNull(namedValues, "namedValues must not be null");
    namedValues.forEach(this::put);
  }

  public NvT getNamedValue(NvT namedValue) {
    return get(namedValue.name());
  }

  public boolean containsNamedValue(NvT namedValue) {
    return containsKey(namedValue.name());
  }

  /** Helper that returns an immutable list of the {@link NamedValue#value()}s in the container. */
  public Collection<ValueT> valuesValue() {
    return values().stream().map(NamedValue::value).toList();
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    DataRecorder recorder = dataRecorder;
    for (NvT namedValue : values()) {
      recorder = namedValue.recordTo(recorder);
    }
    return recorder;
  }
}
