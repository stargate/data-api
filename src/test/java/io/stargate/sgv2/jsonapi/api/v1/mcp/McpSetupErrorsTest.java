package io.stargate.sgv2.jsonapi.api.v1.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkiverse.mcp.server.Cancellation;
import io.quarkiverse.mcp.server.InputRequiredException;
import io.quarkiverse.mcp.server.JsonRpcErrorCodes;
import io.quarkiverse.mcp.server.McpException;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.ToolResponse;
import io.quarkus.security.AuthenticationException;
import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.ForbiddenException;
import io.quarkus.security.UnauthorizedException;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.TestConstants;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.exception.APIException;
import io.stargate.sgv2.jsonapi.exception.APISecurityException;
import io.stargate.sgv2.jsonapi.exception.EmbeddingProviderException;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCache;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCacheSupplier;
import io.stargate.sgv2.jsonapi.util.LoggerTestWrapper;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.inject.Instance;
import jakarta.validation.Validator;
import jakarta.ws.rs.WebApplicationException;
import java.security.Principal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.slf4j.MDC;

/** Setup failures become tool results. Transport and processor failures keep their owners. */
class McpSetupErrorsTest {

  private static final TestConstants TEST_CONSTANTS = new TestConstants();
  private static final String SECRET = "secret-token-from-internal-setup";
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final MeteredCommandProcessor commandProcessor = mock(MeteredCommandProcessor.class);
  private final SchemaObjectCache schemaObjectCache = mock(SchemaObjectCache.class);
  private final EmbeddingProviderFactory embeddingProviderFactory =
      mock(EmbeddingProviderFactory.class);
  private final RequestTenantResolver tenantResolver = mock(RequestTenantResolver.class);
  private final RoutingContext routingContext = mock(RoutingContext.class);
  private final GeneralCommand generalCommand = mock(GeneralCommand.class);
  private final KeyspaceCommand keyspaceCommand = mock(KeyspaceCommand.class);
  private final CollectionCommand collectionCommand = mock(CollectionCommand.class);
  private MockedStatic<ConfigPreLoader> configPreLoader;
  private McpResource resource;

  @BeforeEach
  void setUp() {
    when(generalCommand.commandName()).thenReturn(CommandName.FIND_EMBEDDING_PROVIDERS);
    when(keyspaceCommand.commandName()).thenReturn(CommandName.FIND_COLLECTIONS);
    when(collectionCommand.commandName()).thenReturn(CommandName.COUNT_DOCUMENTS);
    // Supply only application configuration. MCP enablement still comes from real request headers.
    var commandConfig = mock(CommandConfig.class);
    when(commandConfig.get(FeaturesConfig.class)).thenReturn(() -> Map.of());
    configPreLoader = mockStatic(ConfigPreLoader.class);
    configPreLoader.when(ConfigPreLoader::getPreLoadOrEmpty).thenReturn(commandConfig);

    var headers = MultiMap.caseInsensitiveMultiMap().add(ApiFeature.MCP.httpHeaderName(), "true");
    var httpRequest = mock(HttpServerRequest.class);
    when(httpRequest.headers()).thenReturn(headers);
    var routingData = new HashMap<String, Object>();
    when(routingContext.get(anyString()))
        .thenAnswer(invocation -> routingData.get(invocation.getArgument(0)));
    when(routingContext.put(anyString(), any()))
        .thenAnswer(
            invocation -> {
              routingData.put(invocation.getArgument(0), invocation.getArgument(1));
              return routingContext;
            });
    when(routingContext.request()).thenReturn(httpRequest);
    var principal = mock(Principal.class);
    when(principal.getName()).thenReturn("test-token");
    var securityIdentity = mock(SecurityIdentity.class);
    when(securityIdentity.getPrincipal()).thenReturn(principal);
    when(tenantResolver.resolve(any(), any())).thenReturn(TEST_CONSTANTS.TENANT);
    var credentialsResolver = mock(EmbeddingCredentialsResolver.class);
    when(credentialsResolver.resolveEmbeddingCredentials(any(), any()))
        .thenReturn(TEST_CONSTANTS.EMBEDDING_CREDENTIALS);
    var sessionCacheSupplier = mock(CqlSessionCacheSupplier.class);
    when(sessionCacheSupplier.get()).thenReturn(mock(CQLSessionCache.class));
    var schemaCacheSupplier = mock(SchemaObjectCacheSupplier.class);
    when(schemaCacheSupplier.get()).thenReturn(schemaObjectCache);

    resource =
        new McpResource(
            objectMapper,
            mock(Validator.class),
            commandProcessor,
            mock(MeterRegistry.class),
            mock(JsonProcessingMetricsReporter.class),
            sessionCacheSupplier,
            embeddingProviderFactory,
            mock(RerankingProviderFactory.class),
            schemaCacheSupplier,
            () -> routingContext,
            () -> securityIdentity,
            instanceOf(tenantResolver),
            instanceOf(credentialsResolver));
  }

  @AfterEach
  void tearDown() {
    configPreLoader.close();
    MDC.clear();
  }

  @ParameterizedTest
  @EnumSource(CommandScope.class)
  void requestContextFailureIsDeferredAndReturnedAsAToolError(CommandScope scope) throws Exception {
    var failure = SchemaException.Code.UNKNOWN_KEYSPACE.get("keyspace", "missing-keyspace");
    when(tenantResolver.resolve(any(), any())).thenThrow(failure);

    var response = assertDoesNotThrow(() -> call(scope));

    verifyNoInteractions(tenantResolver, schemaObjectCache, commandProcessor);
    assertSetupError(response, failure);
    verifyNoInteractions(schemaObjectCache);
  }

  @Test
  void nullKeyspaceIdentifierBecomesADeferredToolError() throws Exception {
    var response =
        assertDoesNotThrow(
            () ->
                resource.processCommand(
                    () -> resource.buildKeyspaceContext(null, keyspaceCommand), keyspaceCommand));

    verifyNoInteractions(tenantResolver, schemaObjectCache, commandProcessor);
    assertSetupError(response, ServerException.Code.UNEXPECTED_SERVER_ERROR.name());
    verifyNoInteractions(schemaObjectCache);
  }

  @Test
  void nullCollectionIdentifierBecomesADeferredToolError() throws Exception {
    // Collection names are validated inside the schema lookup before any cache or database access.
    doCallRealMethod().when(schemaObjectCache).getTableBased(any(), any(), any(), anyBoolean());
    var response =
        assertDoesNotThrow(
            () ->
                resource.processCollectionCommand(
                    TEST_CONSTANTS.KEYSPACE_NAME, null, collectionCommand));

    verifyNoInteractions(tenantResolver, schemaObjectCache, commandProcessor);
    assertSetupError(response, ServerException.Code.UNEXPECTED_SERVER_ERROR.name());
    verify(schemaObjectCache).getTableBased(any(), any(), any(), anyBoolean());
  }

  @Test
  void databaseSchemaFailureReturnsItsActionableError() throws Exception {
    var failure = APISecurityException.Code.UNAUTHENTICATED_REQUEST.get();
    when(schemaObjectCache.getDatabase(any(), any(), any()))
        .thenReturn(Uni.createFrom().failure(failure));

    assertSetupError(call(CommandScope.DATABASE), failure);
  }

  @Test
  void keyspaceSchemaFailureReturnsItsActionableError() throws Exception {
    var failure = SchemaException.Code.UNKNOWN_KEYSPACE.get("keyspace", "missing-keyspace");
    when(schemaObjectCache.getKeyspace(any(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().failure(failure));

    assertSetupError(call(CommandScope.KEYSPACE), failure);
  }

  @Test
  void collectionSchemaFailureReturnsItsActionableError() throws Exception {
    var failure =
        SchemaException.Code.UNKNOWN_COLLECTION_OR_TABLE.get(
            "keyspace",
            TEST_CONSTANTS.KEYSPACE_NAME,
            "table",
            "missing-collection",
            "allTables",
            "[]");
    when(schemaObjectCache.getTableBased(any(), any(), any(), anyBoolean()))
        .thenReturn(Uni.createFrom().failure(failure));

    assertSetupError(call(CommandScope.COLLECTION), failure);
  }

  @Test
  void providerConstructionFailureReturnsItsActionableError() throws Exception {
    doReturn(Uni.createFrom().item(TEST_CONSTANTS.VECTORIZE_RERANK_COLLECTION_SCHEMA_OBJECT))
        .when(schemaObjectCache)
        .getTableBased(any(), any(), any(), anyBoolean());
    var failure =
        EmbeddingProviderException.Code.EMBEDDING_PROVIDER_UNAVAILABLE.get(
            "errorMessage", "The selected provider is unavailable.");
    when(embeddingProviderFactory.create(
            any(), any(), any(), any(), anyInt(), any(), any(), anyString()))
        .thenThrow(failure);

    var response = assertDoesNotThrow(() -> call(CommandScope.COLLECTION));

    assertSetupError(response, failure);
    verify(embeddingProviderFactory)
        .create(any(), any(), any(), any(), anyInt(), any(), any(), anyString());
  }

  @Test
  void unknownSetupFailureDoesNotExposeExceptionDetails() throws Exception {
    when(schemaObjectCache.getTableBased(any(), any(), any(), anyBoolean()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException(SECRET)));

    var error =
        assertSetupError(
            call(CommandScope.COLLECTION), ServerException.Code.UNEXPECTED_SERVER_ERROR.name());

    assertThat(error.toString()).doesNotContain(SECRET, IllegalStateException.class.getName());
  }

  @Test
  void unexpectedSetupFailureRecordsASafeDiagnostic() throws Exception {
    when(schemaObjectCache.getTableBased(any(), any(), any(), anyBoolean()))
        .thenReturn(Uni.createFrom().failure(new IllegalStateException(SECRET)));
    var requestId = RequestContext.getOrCreateRequestId(routingContext);

    try (var logs = new LoggerTestWrapper(McpRequestLogging.class)) {
      assertSetupError(
          call(CommandScope.COLLECTION), ServerException.Code.UNEXPECTED_SERVER_ERROR.name());
      assertThat(logs.logRecords())
          .filteredOn(record -> record.getMessage().contains("MCP setup failed"))
          .singleElement()
          .satisfies(
              record -> {
                assertThat(record.getMessage())
                    .contains("category=unexpected")
                    .doesNotContain(SECRET);
                assertThat(record.getParameters()).containsExactly(requestId);
                assertThat(record.getThrown()).isNull();
              });
    }
  }

  @Test
  void completionWrappedSetupErrorRetainsItsActionableMessage() throws Exception {
    var failure = SchemaException.Code.UNKNOWN_KEYSPACE.get("keyspace", "missing-keyspace");
    // Cache reads can synchronously surface a failed future through CompletionException.
    doThrow(new CompletionException(failure))
        .when(schemaObjectCache)
        .getTableBased(any(), any(), any(), anyBoolean());

    var response = assertDoesNotThrow(() -> call(CommandScope.COLLECTION));

    assertSetupError(response, failure);
  }

  @Test
  void completionWrappedUnauthorizedFailureRemainsATransportFailure() {
    var failure = new UnauthorizedException("authentication required");
    doThrow(new CompletionException(failure))
        .when(schemaObjectCache)
        .getTableBased(any(), any(), any(), anyBoolean());

    var response = assertDoesNotThrow(() -> call(CommandScope.COLLECTION));

    assertThatThrownBy(() -> response.await().atMost(TIMEOUT)).isSameAs(failure);
    verifyNoInteractions(commandProcessor);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("transportFailures")
  void collectionSetupPreservesTransportAndProtocolFailures(Throwable failure) {
    when(schemaObjectCache.getTableBased(any(), any(), any(), anyBoolean()))
        .thenReturn(Uni.createFrom().failure(failure));

    var response = assertDoesNotThrow(() -> call(CommandScope.COLLECTION));

    assertThatThrownBy(() -> response.await().atMost(TIMEOUT)).isSameAs(failure);
    verifyNoInteractions(commandProcessor);
  }

  private static Stream<Throwable> transportFailures() {
    return Stream.of(
        new SecurityException("resource access denied"),
        new UnauthorizedException("authentication required"),
        new ForbiddenException("access denied"),
        new AuthenticationFailedException("authentication failed"),
        new AuthenticationMarkerFailure(),
        new WebApplicationException("authentication required", 401),
        new WebApplicationException("access denied", 403),
        new McpException("invalid protocol request", JsonRpcErrorCodes.INVALID_REQUEST),
        InputRequiredException.builder().addRootsRequest("roots").build(),
        new Cancellation.OperationCancellationException(),
        new CancellationException("request canceled"));
  }

  @Test
  void processorFailureIsNotConvertedByTheSetupErrorHandler() {
    when(schemaObjectCache.getKeyspace(any(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().item(TEST_CONSTANTS.KEYSPACE_SCHEMA_OBJECT));
    var failure = new IllegalStateException("processor owns this failure");
    when(commandProcessor.processCommand(any(), eq(keyspaceCommand)))
        .thenReturn(Uni.createFrom().failure(failure));

    var response = call(CommandScope.KEYSPACE);

    assertThatThrownBy(() -> response.await().atMost(TIMEOUT)).isSameAs(failure);
    verify(commandProcessor).processCommand(any(), eq(keyspaceCommand));
  }

  @Test
  void validCallSucceedsAfterASetupErrorOnTheSameResource() throws Exception {
    var failure = SchemaException.Code.UNKNOWN_KEYSPACE.get("keyspace", "missing-keyspace");
    when(schemaObjectCache.getKeyspace(any(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().failure(failure))
        .thenReturn(Uni.createFrom().item(TEST_CONSTANTS.KEYSPACE_SCHEMA_OBJECT));

    assertSetupError(call(CommandScope.KEYSPACE), failure);

    assertNextKeyspaceCallSucceeds();
  }

  @Test
  void validCallSucceedsAfterATransportDenialOnTheSameResource() throws Exception {
    var failure = new UnauthorizedException("authentication required");
    when(schemaObjectCache.getKeyspace(any(), any(), any(), eq(true)))
        .thenReturn(Uni.createFrom().failure(failure))
        .thenReturn(Uni.createFrom().item(TEST_CONSTANTS.KEYSPACE_SCHEMA_OBJECT));

    var response = call(CommandScope.KEYSPACE);
    assertThatThrownBy(() -> response.await().atMost(TIMEOUT)).isSameAs(failure);
    verifyNoInteractions(commandProcessor);

    assertNextKeyspaceCallSucceeds();
  }

  private void assertNextKeyspaceCallSucceeds() throws Exception {
    var result =
        CommandResult.statusOnlyBuilder(RequestTracing.NO_OP)
            .addStatus(CommandStatus.OK, 1)
            .build();
    when(commandProcessor.processCommand(any(), eq(keyspaceCommand)))
        .thenReturn(Uni.createFrom().item(result));

    var response = call(CommandScope.KEYSPACE).await().atMost(TIMEOUT);

    assertThat(response.isError()).isFalse();
    assertThat(readableResult(response)).isEqualTo(objectMapper.valueToTree(result));
    verify(commandProcessor).processCommand(any(), eq(keyspaceCommand));
  }

  private Uni<ToolResponse> call(CommandScope scope) {
    return switch (scope) {
      case DATABASE ->
          resource.processCommand(
              () -> resource.buildDatabaseContext(generalCommand), generalCommand);
      case KEYSPACE ->
          resource.processCommand(
              () -> resource.buildKeyspaceContext(TEST_CONSTANTS.KEYSPACE_NAME, keyspaceCommand),
              keyspaceCommand);
      case COLLECTION ->
          resource.processCollectionCommand(
              TEST_CONSTANTS.KEYSPACE_NAME, TEST_CONSTANTS.COLLECTION_NAME, collectionCommand);
    };
  }

  private void assertSetupError(Uni<ToolResponse> responseUni, APIException failure)
      throws Exception {
    var error = assertSetupError(responseUni, failure.code);
    assertThat(error.path("message").asText()).isEqualTo(failure.body);
  }

  private JsonNode assertSetupError(Uni<ToolResponse> responseUni, String code) throws Exception {
    var response = responseUni.await().atMost(TIMEOUT);
    assertThat(response.isError()).isTrue();
    var readable = readableResult(response);
    assertThat(readable).isEqualTo(objectMapper.valueToTree(response.structuredContent()));
    assertThat(readable.path("errors")).hasSize(1);
    var error = readable.path("errors").get(0);
    assertThat(error.path("errorCode").asText()).isEqualTo(code);
    assertThat(error.path("message").asText()).isNotBlank();
    assertThat(response._meta()).isEmpty();
    verifyNoInteractions(commandProcessor);
    return error;
  }

  private JsonNode readableResult(ToolResponse response) throws Exception {
    assertThat(response.content()).hasSize(1);
    var text = assertInstanceOf(TextContent.class, response.content().getFirst()).text();
    return objectMapper.readTree(text);
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> instanceOf(T value) {
    Instance<T> instance = mock(Instance.class);
    when(instance.get()).thenReturn(value);
    return instance;
  }

  private enum CommandScope {
    DATABASE,
    KEYSPACE,
    COLLECTION
  }

  /** Quarkus treats this marker as authentication even without a SecurityException superclass. */
  private static final class AuthenticationMarkerFailure extends RuntimeException
      implements AuthenticationException {}
}
