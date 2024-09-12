package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * Base for all containers that hold {@link NamedValue}s.
 *
 * <p>Useful so we can deal with a container of {@link NamedValue}s in a generic way, provides some
 * helper methods, and implements {@link PrettyPrintable}.
 *
 * @param <NameT> The type of the name, this is the key in the map
 * @param <ValueT> The type of the value stored in the {@link NamedValue}
 * @param <NvT> The type of the {@link NamedValue} stored in the map
 */
public interface NamedValueContainer<NameT, ValueT, NvT extends NamedValue<NameT, ValueT>>
    extends Map<NameT, NvT>, PrettyPrintable {

  /**
   * Helper to add a {@link NamedValue} to the container, keyed on the {@link NamedValue#name()}.
   *
   * @param namedValue The {@link NamedValue} to add
   * @return The previous value associated with the name, or null if there was no mapping for the
   *     name
   */
  default NamedValue<NameT, ValueT> put(NvT namedValue) {
    return put(namedValue.name(), namedValue);
  }

  default void putAll(Collection<NvT> namedValues) {
    Objects.requireNonNull(namedValues, "namedValues must not be null");
    namedValues.forEach(this::put);
  }

  /** Helper that returns an immutable list of the {@link NamedValue#value()}s in the container. */
  default Collection<ValueT> valuesValue() {
    return values().stream().map(NamedValue::value).toList();
  }

  default String toString(boolean pretty) {
    return toString(new PrettyToStringBuilder(getClass(), pretty)).toString();
  }

  default PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
    var sb = prettyToStringBuilder.beginSubBuilder(getClass());
    return toString(sb).endSubBuilder();
  }

  default PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    forEach((key, value) -> prettyToStringBuilder.append(key.toString(), value));
    return prettyToStringBuilder;
  }
}
