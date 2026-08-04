package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.testresource.NoGlobalResourcesTestProfile;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(DatabaseReadinessResourceTest.AstraProfile.class)
public class DatabaseReadinessResourceTest {

  private static final String TENANT_ID = "60b5dccb-e91d-4a60-987b-7588cd8aa1e3";
  private static final String REGION = "us-west-2";
  private static final String ASTRA_HOST = TENANT_ID + "-" + REGION + ".apps.astra.datastax.com";
  private static final String TOKEN = "astra-canary-token";
  private static final String SLA_USER_AGENT = "Datastax-SLA-Checker";

  @InjectMock CqlSessionCacheSupplier sessionCacheSupplier;

  private CQLSessionCache sessionCache;
  private CqlSession session;

  @BeforeEach
  public void setup() {
    sessionCache = mock(CQLSessionCache.class);
    session = mock(CqlSession.class);
    when(sessionCacheSupplier.get()).thenReturn(sessionCache);
  }

  @Test
  public void missingTokenIsRejectedBeforeDatabaseAccess() {
    given()
        .header("Host", ASTRA_HOST)
        .header("User-Agent", SLA_USER_AGENT)
        .when()
        .get(DatabaseReadinessResource.BASE_PATH)
        .then()
        .statusCode(401);

    verifyNoInteractions(sessionCache);
  }

  @Test
  public void astraRequestUsesResolvedTenantTokenAndSlaUserAgent() {
    var resultSet = mock(AsyncResultSet.class);
    var capturedContext = new AtomicReference<RequestContextSnapshot>();
    when(sessionCache.getSession(any(RequestContext.class)))
        .thenAnswer(
            invocation -> {
              RequestContext context = invocation.getArgument(0);
              capturedContext.set(
                  new RequestContextSnapshot(
                      context.tenant().toString(),
                      context.tenant().region(),
                      context.authToken(),
                      context.userAgent().toString()));
              return Uni.createFrom().item(session);
            });
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenReturn(CompletableFuture.completedFuture(resultSet));

    authenticatedRequest()
        .when()
        .get(DatabaseReadinessResource.BASE_PATH)
        .then()
        .statusCode(200)
        .body("status", equalTo("UP"));

    assertThat(capturedContext.get())
        .isEqualTo(new RequestContextSnapshot(TENANT_ID, REGION, TOKEN, SLA_USER_AGENT));
  }

  @Test
  public void databaseFailureReturnsServiceUnavailableWithoutDetails() {
    when(sessionCache.getSession(any(RequestContext.class)))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException("sensitive failure")));

    var response =
        authenticatedRequest()
            .when()
            .get(DatabaseReadinessResource.BASE_PATH)
            .then()
            .statusCode(503)
            .body("status", equalTo("DOWN"))
            .extract()
            .asString();

    assertThat(response).doesNotContain("sensitive failure", TOKEN, TENANT_ID);
  }

  @Test
  public void invalidTokenFailureReturnsUnauthorizedWithoutDetails() {
    when(sessionCache.getSession(any(RequestContext.class)))
        .thenThrow(new UnauthorizedException("sensitive credential failure"));

    var response =
        authenticatedRequest()
            .when()
            .get(DatabaseReadinessResource.BASE_PATH)
            .then()
            .statusCode(401)
            .body("status", equalTo("DOWN"))
            .extract()
            .asString();

    assertThat(response).doesNotContain("sensitive credential failure", TOKEN, TENANT_ID);
  }

  @Test
  public void databaseAuthenticationFailureReturnsUnauthorizedWithoutDetails() {
    var authenticationFailure =
        APISecurityException.Code.UNAUTHENTICATED_REQUEST.withPreformattedMessage(
            "sensitive database authentication failure");
    when(sessionCache.getSession(any(RequestContext.class)))
        .thenReturn(Uni.createFrom().failure(authenticationFailure));

    var response =
        authenticatedRequest()
            .when()
            .get(DatabaseReadinessResource.BASE_PATH)
            .then()
            .statusCode(401)
            .body("status", equalTo("DOWN"))
            .extract()
            .asString();

    assertThat(response)
        .doesNotContain("sensitive database authentication failure", TOKEN, TENANT_ID);
  }

  private io.restassured.specification.RequestSpecification authenticatedRequest() {
    return given()
        .header("Host", ASTRA_HOST)
        .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, TOKEN)
        .header("User-Agent", SLA_USER_AGENT);
  }

  private record RequestContextSnapshot(
      String tenantId, String region, String authToken, String userAgent) {}

  public static class AstraProfile implements NoGlobalResourcesTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "stargate.jsonapi.operations.database-config.type",
          "ASTRA",
          "stargate.multi-tenancy.enabled",
          "true",
          "stargate.multi-tenancy.tenant-resolver.type",
          "subdomain",
          "stargate.multi-tenancy.tenant-resolver.subdomain.max-chars",
          "36",
          "stargate.jsonapi.operations.sla-user-agent",
          SLA_USER_AGENT);
    }
  }
}
