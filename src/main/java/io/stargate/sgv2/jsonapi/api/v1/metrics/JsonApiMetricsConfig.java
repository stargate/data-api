package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.constraints.NotBlank;

@ConfigMapping(prefix = "stargate.jsonapi.metric")
public interface JsonApiMetricsConfig {
  @NotBlank
  @WithDefault("error.class")
  String errorClass();

  @NotBlank
  @WithDefault("error.code")
  String errorCode();

  @NotBlank
  @WithDefault("command")
  String command();

  @NotBlank
  @WithDefault("vector.enabled")
  String vectorEnabled();

  @NotBlank
  @WithDefault("sort.type")
  String sortType();

  @NotBlank
  @WithDefault("json.shredding.type")
  String jsonShreddingTypeTag();

  @NotBlank
  @WithDefault("json.string.length")
  String jsonStringLength();

  @NotBlank
  @WithDefault("json.string.bytes")
  String jsonStringBytes();

  @NotBlank
  @WithDefault("json.shredding.metrics")
  String jsonShreddingMetricsName();

  @NotBlank
  @WithDefault("command.processor.process")
  String metricsName();

  /** List of values that can be used as value for metrics sort_type. */
  enum SortType {
    // Uses vertor search sorting for document resolution
    SIMILARITY_SORT,
    // Uses vector search with filters for document resolution
    SIMILARITY_SORT_WITH_FILTERS,
    // Uses sort based on a document field for document resolution
    SORT_BY_FIELD,
    // No sort queries
    NONE
  }
}
