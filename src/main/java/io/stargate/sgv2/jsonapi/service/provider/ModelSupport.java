package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * By default, model is supported and has no message. So if model-support is not configured in the
 * config source, it will be supported by default.
 *
 * <p>If the model is deprecated or EOF, it will be marked in the config source and been mapped.
 *
 * <p>If message is not configured in config source, it will be Optional.empty().
 */
public interface ModelSupport {
  @JsonProperty
  @WithDefault("SUPPORTED")
  SupportStatus status();

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Optional<String> message();

  enum SupportStatus {
    SUPPORTED,
    DEPRECATED,
    END_OF_LIFE
  }

  record ModelSupportImpl(ModelSupport.SupportStatus status, Optional<String> message)
      implements ModelSupport {}
}
