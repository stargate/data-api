package io.stargate.sgv2.jsonapi.api.request.tenant;

import static org.assertj.core.api.Assertions.assertThat;
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

/** Tests for {@link SubdomainTenantResolver}, focused on Astra-style endpoint URLs. */
public class SubdomainTenantResolverTests {

  // Astra tenant ids are 36-char UUIDs; production config caps tenant extraction at the UUID
  // length. See
  // https://github.com/riptano/serverless-platform-charts/blob/master/stargate-apis/chart/charts/jsonapi/templates/deployment.yaml#L120
  private static final int TENANT_UUID_LENGTH = 36;
  private static final String TENANT_UUID = "11111111-2222-3333-4444-555555555555";
  // https://github.com/riptano/serverless-platform-charts/blob/master/stargate-apis/chart/charts/jsonapi/templates/deployment.yaml#L122
  private static final String SUBDOMAIN_REGEX = "[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}";

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

  private static RoutingContext routingContextWithHost(String host) {
    var request = mock(HttpServerRequest.class);
    when(request.host()).thenReturn(host);
    var context = mock(RoutingContext.class);
    when(context.request()).thenReturn(request);
    return context;
  }

  @Test
  public void extractsTenantAndRegionFromAstraEndpoint() {
    var resolver = resolver(OptionalInt.of(TENANT_UUID_LENGTH), Optional.empty());
    var context = routingContextWithHost(TENANT_UUID + "-us-west-2.apps.astra.datastax.com");

    Tenant tenant = resolver.resolve(context, null);

    assertThat(tenant.toString()).isEqualTo(TENANT_UUID);
    assertThat(tenant.region()).isEqualTo("us-west-2");
  }

  @Test
  public void extractsMultiSegmentRegion() {
    // Some Azure-style regions have no hyphens, others (like us-west-2) carry multiple segments.
    // Anything after the tenant + '-' separator is treated as a single region string.
    var resolver = resolver(OptionalInt.of(TENANT_UUID_LENGTH), Optional.empty());
    var context = routingContextWithHost(TENANT_UUID + "-eu-central-1.apps.astra.datastax.com");

    Tenant tenant = resolver.resolve(context, null);

    assertThat(tenant.region()).isEqualTo("eu-central-1");
  }

  @Test
  public void singleSegmentRegion() {
    var resolver = resolver(OptionalInt.of(TENANT_UUID_LENGTH), Optional.empty());
    var context = routingContextWithHost(TENANT_UUID + "-southcentralus.apps.astra.datastax.com");

    Tenant tenant = resolver.resolve(context, null);

    assertThat(tenant.region()).isEqualTo("southcentralus");
  }

  @Test
  public void noRegionInSubdomainFallsBackToUnknown() {
    // No '-region' suffix on the subdomain: tenantId fills the whole subdomain.
    var resolver = resolver(OptionalInt.of(TENANT_UUID_LENGTH), Optional.empty());
    var context = routingContextWithHost(TENANT_UUID + ".apps.astra.datastax.com");

    Tenant tenant = resolver.resolve(context, null);

    assertThat(tenant.toString()).isEqualTo(TENANT_UUID);
    assertThat(tenant.region()).isEqualTo(Tenant.UNKNOWN_REGION);
  }

  @Test
  public void noMaxCharsTreatsWholeSubdomainAsTenant() {
    // Without maxChars the resolver cannot tell where the tenant ends and the region begins,
    // so the whole subdomain becomes the tenant id and region falls back to UNKNOWN_REGION.
    var resolver = resolver(OptionalInt.empty(), Optional.empty());
    var context = routingContextWithHost(TENANT_UUID + "-us-west-2.apps.astra.datastax.com");

    Tenant tenant = resolver.resolve(context, null);

    assertThat(tenant.toString()).isEqualTo(TENANT_UUID + "-us-west-2");
    assertThat(tenant.region()).isEqualTo(Tenant.UNKNOWN_REGION);
  }
}
