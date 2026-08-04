package io.stargate.sgv2.jsonapi.metrics;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.DatabaseReadinessResource;
import io.stargate.sgv2.jsonapi.api.v1.metrics.MetricsConfig;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class TenantRequestMetricsFilterTest {

  @Test
  public void databaseReadinessIsNotCountedAsTenantTraffic() {
    var meterRegistry = mock(MeterRegistry.class);
    var dataApiRequestContext = mock(RequestContext.class);
    var metricsConfig = mock(MetricsConfig.class);
    var tenantRequestConfig = mock(MetricsConfig.TenantRequestCounterConfig.class);
    when(metricsConfig.tenantRequestCounter()).thenReturn(tenantRequestConfig);
    when(tenantRequestConfig.enabled()).thenReturn(true);

    var requestContext = mock(ContainerRequestContext.class);
    var uriInfo = mock(UriInfo.class);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getRequestUri())
        .thenReturn(URI.create("http://localhost" + DatabaseReadinessResource.BASE_PATH));

    var filter =
        new TenantRequestMetricsFilter(meterRegistry, dataApiRequestContext, metricsConfig);
    filter.record(requestContext, mock(ContainerResponseContext.class));

    verifyNoInteractions(meterRegistry, dataApiRequestContext);
  }
}
