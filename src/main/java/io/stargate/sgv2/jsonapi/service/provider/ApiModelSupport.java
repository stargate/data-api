package io.stargate.sgv2.jsonapi.service.provider;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.smallrye.config.WithDefault;
import java.util.Optional;

/**
 * By default, model is supported and has no message. So if api-model-support is not configured in
 * the config source, it will be supported by default.
 *
 * <p>If the model is deprecated or EOF, it will be marked in the config source and been mapped.
 *
 * <p>If message is not configured in config source, it will be Optional.empty().
 */
public interface ApiModelSupport {
  @JsonProperty
  @WithDefault("SUPPORTED")
  SupportStatus status();

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Optional<String> message();

  /** Enumeration of support status for an embedding or reranking model. */
  enum SupportStatus {
    /** The model is supported and can be used when creating new Collections and Tables. */
    SUPPORTED,
    /**
     * The model is deprecated and may be removed in future versions. Data API supports read and
     * write on DEPRECATED model, createCollection and CreateTable are forbidden.
     */
    DEPRECATED,
    /**
     * The model is no longer supported and should not be used. Data API does not support read,
     * write, createCollection, createTable for END_OF_LIFE model.
     */
    END_OF_LIFE
  }

  record ApiModelSupportImpl(ApiModelSupport.SupportStatus status, Optional<String> message)
      implements ApiModelSupport {}
}
