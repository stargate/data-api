package io.stargate.sgv2.jsonapi.api.request.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.config.MultiTenancyConfig;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import java.util.OptionalInt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests for {@link SubdomainTenantResolver}, focused on Astra-style endpoint URLs. */
public class SubdomainTenantResolverTests {

  // Astra tenant ids are 36-char UUIDs; production config caps tenant extraction at the UUID
  // length. See
  // https://github.com/riptano/serverless-platform-charts/blob/master/stargate-apis/chart/charts/jsonapi/templates/deployment.yaml#L120
  private static final int TENANT_UUID_LENGTH = 36;
  private static final String TENANT_UUID = "11111111-2222-3333-4444-555555555555";
  // https://github.com/riptano/serverless-platform-charts/blob/master/stargate-apis/chart/charts/jsonapi/templates/deployment.yaml#L122
  private static final String SUBDOMAIN_REGEX =
      "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

  @BeforeEach
  public void initTenantFactory() {
    TenantFactory.initialize(DatabaseType.ASTRA);
  }

  @AfterEach
  public void resetTenantFactory() {
    TenantFactory.reset();
  }

  private static SubdomainTenantResolver resolver(OptionalInt maxChars, Optional<String> regex) {
    var config = mock(MultiTenancyConfig.TenantResolverConfig.SubdomainTenantResolverConfig.class);
    when(config.maxChars()).thenReturn(maxChars);
    when(config.regex()).thenReturn(regex);
    return new SubdomainTenantResolver(config);
  }

  /** Resolver configured the way the production Helm chart configures it. */
  private static SubdomainTenantResolver prodConfigResolver() {
    return resolver(OptionalInt.of(TENANT_UUID_LENGTH), Optional.of(SUBDOMAIN_REGEX));
  }

  private static RoutingContext routingContextWithHost(String host) {
    var request = mock(HttpServerRequest.class);
    when(request.host()).thenReturn(host);
    var context = mock(RoutingContext.class);
    when(context.request()).thenReturn(request);
    return context;
  }

  /**
   * Sample of representative real Astra region names — AWS (multi-segment hyphenated), Azure (no
   * hyphens, sometimes with trailing digit), and GCP (hyphenated with trailing digit, sometimes
   * multi-segment). The resolver does not parse the region string; anything after the {@code
   * tenantId + '-'} separator is returned verbatim.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        // AWS
        "us-west-2",
        "eu-central-1",
        "ap-southeast-2",
        "ca-central-1",
        // Azure
        "eastus",
        "southcentralus",
        "uksouth",
        "australiacentral2",
        // GCP
        "us-central1",
        "europe-west4",
        "northamerica-northeast1",
        "asia-northeast3",
      })
  public void extractsTenantAndRegionFromAstraEndpoint(String region) {
    var context = routingContextWithHost(TENANT_UUID + "-" + region + ".apps.astra.datastax.com");

    Tenant tenant = prodConfigResolver().resolve(context, null);

    assertThat(tenant.toString()).isEqualTo(TENANT_UUID);
    assertThat(tenant.region()).isEqualTo(region);
  }

  @Test
  public void noRegionInSubdomainFallsBackToUnknown() {
    // Subdomain is exactly the tenant UUID — no '-region' suffix.
    var context = routingContextWithHost(TENANT_UUID + ".apps.astra.datastax.com");

    Tenant tenant = prodConfigResolver().resolve(context, null);

    assertThat(tenant.toString()).isEqualTo(TENANT_UUID);
    assertThat(tenant.region()).isEqualTo(Tenant.UNKNOWN_REGION);
  }

  @Test
  public void invalidTenantSubdomainRejectedByRegex() {
    // Subdomain doesn't match the UUID regex — resolver passes null to TenantFactory, which for
    // ASTRA surfaces as IllegalArgumentException ("tenantId must not be empty").
    var context = routingContextWithHost("not-a-uuid.apps.astra.datastax.com");

    assertThatThrownBy(() -> prodConfigResolver().resolve(context, null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void hostWithoutSubdomainRejected() {
    // No '.' in host — no subdomain to parse → resolver passes null to TenantFactory.
    var context = routingContextWithHost("localhost");

    assertThatThrownBy(() -> prodConfigResolver().resolve(context, null))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
