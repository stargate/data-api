package io.stargate.sgv2.jsonapi.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.spi.Converter;

@ApplicationScoped
public class NullableBooleanConverter implements Converter<Boolean> {

  @Override
  public Boolean convert(String value) {
    if (value == null || value.isBlank()) {
      return null; // Return null for empty or null strings
    }
    if ("true".equals(value)) {
      return true;
    }
    if ("false".equals(value)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Invalid `Boolean` value: '" + value + "'. Expected 'true' or 'false'.");
  }
}
