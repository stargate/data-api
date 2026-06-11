package io.stargate.sgv2.jsonapi.service.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.BillingConfig;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class BillingResponseFilterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final TestConstants TEST_CONSTANTS = new TestConstants();

  private record BillingAndFeatures(Billing billing, ApiFeatures apiFeatures) {}

  private static BillingAndFeatures newBillingWith(boolean logging, boolean response) {
    BillingConfig config = mock(BillingConfig.class);
    when(config.product()).thenReturn("serverless");
    when(config.resourceType()).thenReturn("serverless_database");
    when(config.internalModelProviders()).thenReturn(List.of("nvidia"));
    when(config.enabledEventTypes()).thenReturn(Optional.empty());

    FeaturesConfig featuresConfig = mock(FeaturesConfig.class);
    Map<ApiFeature, String> flags = new HashMap<>();
    flags.put(ApiFeature.BILLING_EVENTS_LOGGING, String.valueOf(logging));
    flags.put(ApiFeature.BILLING_EVENTS_RESPONSE, String.valueOf(response));
    when(featuresConfig.flags()).thenReturn(flags);

    ApiFeatures apiFeatures = ApiFeatures.fromConfigAndRequest(featuresConfig, null);
    // Billing.create picks DefaultBilling when either flag is on (NO_OP only when both off) — the
    // same dispatch the filter relies on in production.
    return new BillingAndFeatures(Billing.create(config, apiFeatures), apiFeatures);
  }

  private static ModelUsage usage() {
    return new ModelUsage(
        ModelProvider.NVIDIA,
        ModelType.EMBEDDING,
        "test-model",
        TEST_CONSTANTS.TENANT,
        ModelInputType.INDEX,
        10,
        20,
        100,
        200,
        1000L);
  }

  private static BillingResponseFilter filterFor(Billing billing, ApiFeatures apiFeatures) {
    RequestContext rc = mock(RequestContext.class);
    when(rc.billing()).thenReturn(billing);
    when(rc.apiFeatures()).thenReturn(apiFeatures);
    return new BillingResponseFilter(rc);
  }

  private static ContainerResponseContext responseContextWithHeaders(
      MultivaluedMap<String, Object> headers) {
    ContainerResponseContext response = mock(ContainerResponseContext.class);
    when(response.getHeaders()).thenReturn(headers);
    return response;
  }

  @Test
  void addsHeaderWhenFeatureOnAndEventsPresent() throws Exception {
    BillingAndFeatures bf = newBillingWith(false, true);
    bf.billing().emitEvent(usage());
    BillingResponseFilter filter = filterFor(bf.billing(), bf.apiFeatures());

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    filter.addBillingHeader(responseContextWithHeaders(headers));

    Object headerValue = headers.getFirst(BillingResponseFilter.BILLING_EVENTS_HEADER);
    assertThat(headerValue).isNotNull();
    JsonNode parsed = MAPPER.readTree(headerValue.toString());
    assertThat(parsed.isArray()).isTrue();
    assertThat(parsed.size()).isEqualTo(3);
    assertThat(parsed.get(0).get("event_type").asText()).isEqualTo("internal_model_total_tokens");
  }

  @Test
  void skipsHeaderWhenFeatureOff() {
    // RESPONSE off — header must not be added even if LOGGING was on for this request.
    BillingAndFeatures bf = newBillingWith(true, false);
    bf.billing().emitEvent(usage());
    BillingResponseFilter filter = filterFor(bf.billing(), bf.apiFeatures());

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    ContainerResponseContext response = responseContextWithHeaders(headers);
    filter.addBillingHeader(response);

    assertThat(headers.containsKey(BillingResponseFilter.BILLING_EVENTS_HEADER)).isFalse();
    // We should never touch the headers either (early return saves the work).
    verify(response, never()).getHeaders();
  }

  @Test
  void skipsHeaderWhenNoEventsCollected() {
    // RESPONSE on, but no emitEvent calls — header skipped because buffer is empty.
    BillingAndFeatures bf = newBillingWith(false, true);
    BillingResponseFilter filter = filterFor(bf.billing(), bf.apiFeatures());

    MultivaluedMap<String, Object> headers = new MultivaluedHashMap<>();
    filter.addBillingHeader(responseContextWithHeaders(headers));

    assertThat(headers.containsKey(BillingResponseFilter.BILLING_EVENTS_HEADER)).isFalse();
  }
}
