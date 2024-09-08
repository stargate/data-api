package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

public abstract class NamedValue<NameT, ValueT> implements PrettyPrintable {

  protected final NameT name;
  protected final ValueT value;

  protected NamedValue(NameT name, ValueT value) {
    this.name = name;
    this.value = value;
  }

  public NameT name() {
    return name;
  }

  public ValueT value() {
    return value;
  }

  @Override
  public PrettyToStringBuilder appendTo(PrettyToStringBuilder prettyToStringBuilder) {
    return prettyToStringBuilder.beginSubBuilder(getClass())
        .append("name", name)
        .append("value", value)
        .endSubBuilder();
  }
}

