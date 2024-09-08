package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public interface NamedValueContainer <NameT, ValueT, NvT extends NamedValue<NameT, ValueT>>
    extends Map<NameT, NvT>, PrettyPrintable {

  default NamedValue<NameT, ValueT> put(NvT namedValue) {
    return put(namedValue.name(), namedValue);
  }

  default void putAll(Collection<NvT> namedValues) {
    Objects.requireNonNull(namedValues, "namedValues must not be null");
    namedValues.forEach(this::put);
  }


  default Collection<ValueT> valuesValue() {
    return values().stream()
        .map(NamedValue::value)
        .toList();
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
