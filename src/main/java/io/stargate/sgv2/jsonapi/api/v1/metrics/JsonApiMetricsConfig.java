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
  @WithDefault("command.processor.process")
  String metricsName();
}
