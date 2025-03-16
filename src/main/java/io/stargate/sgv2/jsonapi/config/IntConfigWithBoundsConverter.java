package io.stargate.sgv2.jsonapi.config;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.spi.Converter;

@ApplicationScoped
public class IntConfigWithBoundsConverter implements Converter<IntConfigWithBounds> {

  @Override
  public IntConfigWithBounds convert(String value) {
    // Example: "0,1,10" => min=0, default=1, max=10
    if (value == null || value.isBlank()) {
      // If it's null, we can either fail or default.
      throw new IllegalArgumentException("DefaultIntWithBounds - value cannot be null or blank");
    }
    String[] parts = value.split(",");
    if (parts.length == 1) {
      return new IntConfigWithBounds(Integer.MIN_VALUE, Integer.parseInt(parts[0]), Integer.MAX_VALUE);
    }
    if (parts.length == 3) {
      return new IntConfigWithBounds(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    }

    throw new IllegalArgumentException("Expected 1 or 3 parts for min,default,max but got: " + value);
  }
}
