package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.jsonapi.service.provider.BillingEventType;
import java.util.List;
import java.util.Set;

/** Configuration for the billing event pipeline. */
@ConfigMapping(prefix = "stargate.jsonapi.billing")
public interface BillingConfig {

  @WithDefault("serverless")
  String product();

  @WithDefault("serverless_database")
  String resourceType();

  /**
   * Provider API names (matching {@link
   * io.stargate.sgv2.jsonapi.service.provider.ModelProvider#apiName()}) that are considered
   * "internal" — i.e. infrastructure we host and pay for ourselves (e.g. the Astra GPU plane).
   * Anything not in this list is treated as external. Controls whether a model call's billing
   * events are emitted with the {@code internal_*} or {@code external_*} event types.
   */
  @WithDefault("nvidia")
  List<String> internalModelProviders();

  /**
   * The set of {@link BillingEventType} values to emit. Events whose type is not in this set are
   * dropped silently. Defaults to all six event types.
   */
  @WithDefault(
      "INTERNAL_MODEL_TOTAL_TOKENS,EXTERNAL_MODEL_TOTAL_TOKENS,"
          + "INTERNAL_MODEL_EGRESS_BYTES,EXTERNAL_MODEL_EGRESS_BYTES,"
          + "INTERNAL_MODEL_INGRESS_BYTES,EXTERNAL_MODEL_INGRESS_BYTES")
  Set<BillingEventType> enabledEventTypes();
}
