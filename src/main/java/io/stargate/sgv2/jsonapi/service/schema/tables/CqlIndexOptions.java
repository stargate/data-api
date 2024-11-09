package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import java.util.Map;
import java.util.Objects;

/**
 * Helper to read the optionsl from {@link IndexMetadata#getOptions()} see the {@link
 * IndexFactoryFromCql} for example of these
 */
public enum CqlIndexOptions {
  CLASS("class_name"),
  TARGET("target"),
  SIMILARITY_FUNCTION("similarity_function");

  private final String optionName;

  private CqlIndexOptions(String optionName) {
    this.optionName = optionName;
  }

  /**
   * Reads the option from the index options map that should be obtained from {@link
   * IndexMetadata#getOptions()}
   *
   * @param options Map to read from.
   * @return Raw result from the map, this may be null.
   */
  public String readFrom(Map<String, String> options) {
    Objects.requireNonNull(options, "options must not be null");
    return options.get(optionName);
  }
}
