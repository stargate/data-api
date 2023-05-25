package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

@ConfigMapping(prefix = "stargate.metrics")
public interface JsonApiMetricsConfig {
  @NotNull
  @Valid
  JsonApiMetricsConfig.CustomMetricsConfig customMetricsConfig();

  public interface CustomMetricsConfig {
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
}
