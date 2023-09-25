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
  @WithDefault("command.processor.process")
  String metricsName();

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
