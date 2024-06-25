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

  /**
   * Metric name for the count of bytes written in JSON operations, used to monitor the size of data
   * written by a command.
   *
   * @return metric name for bytes written count.
   */
  @NotBlank
  @WithDefault("json.bytes.written")
  String jsonBytesWritten();

  /**
   * Metric name for the count of bytes read in JSON operations, used to monitor the size of data
   * read by a command.
   *
   * @return metric name for bytes read count.
   */
  @NotBlank
  @WithDefault("json.bytes.read")
  String jsonBytesRead();

  /**
   * Metric name for the count of JSON write operations, indicating the number of write actions
   * performed by a command.
   *
   * @return metric name for write operations count.
   */
  @NotBlank
  @WithDefault("json.docs.written")
  String jsonDocsWritten();

  /**
   * Metric name for the count of JSON read operations, indicating the number of read actions
   * performed by a command.
   *
   * @return metric name for read operations count.
   */
  @NotBlank
  @WithDefault("json.docs.read")
  String jsonDocsRead();

  @NotBlank
  @WithDefault("command.processor.process")
  String metricsName();

  @NotBlank
  @WithDefault("vectorize.call.duration")
  String vectorizeCallDurationMetrics();

  @NotBlank
  @WithDefault("vectorize.input.bytes")
  String vectorizeInputBytesMetrics();

  @NotBlank
  @WithDefault("embedding.provider")
  String embeddingProvider();

  @NotBlank
  @WithDefault("index.usage.count")
  String indexUsageCounterMetrics();

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
