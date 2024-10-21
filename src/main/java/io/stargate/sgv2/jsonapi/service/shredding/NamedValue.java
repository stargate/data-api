package io.stargate.sgv2.jsonapi.service.shredding;

import io.stargate.sgv2.jsonapi.util.PrettyPrintable;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;

/**
 * Abstract idea of a value that has a name.
 *
 * <p>In the API this could be:
 *
 * <ul>
 *   <li>{@link JsonNamedValue} value shredded from a document with the Java value returned from
 *       Jackson, or a value ready to be used to create a JSON document.
 *   <li>{@link CqlNamedValue} that is ready to be passed to the driver for inserting or filtering
 *       or, or a value read from the driver
 * </ul>
 *
 * @param <NameT> The type of the name of the value
 * @param <ValueT> The type of the value
 */
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
  public String toString() {
    return toString(false);
  }

  @Override
  public PrettyToStringBuilder toString(PrettyToStringBuilder prettyToStringBuilder) {
    prettyToStringBuilder.append("name", name).append("value", value);
    return prettyToStringBuilder;
  }
}
