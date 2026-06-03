package io.stargate.sgv2.jsonapi.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.jsonapi.service.provider.BillingEventType;
import java.util.List;
import java.util.Optional;
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
   * dropped. When the property is not configured, all values of {@link BillingEventType} are
   * enabled. Explicitly setting it to an empty value disables all billing events.
   *
   * <p>Examples:
   *
   * <pre>
   * # Not specified (Optional.empty() → all event types enabled):
   * stargate:
   *   jsonapi:
   *     billing:
   *       product: serverless
   *
   * # Specified but empty (Optional of empty set → no event types enabled):
   * stargate:
   *   jsonapi:
   *     billing:
   *       enabled-event-types: []
   *
   * # Specified and enabling three event types:
   * stargate:
   *   jsonapi:
   *     billing:
   *       enabled-event-types:
   *         - INTERNAL_MODEL_TOTAL_TOKENS
   *         - EXTERNAL_MODEL_TOTAL_TOKENS
   *         - INTERNAL_MODEL_EGRESS_BYTES
   * </pre>
   */
  Optional<Set<BillingEventType>> enabledEventTypes();
}
