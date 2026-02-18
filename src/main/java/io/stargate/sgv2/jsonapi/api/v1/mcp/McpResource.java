package io.stargate.sgv2.jsonapi.api.v1.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.security.identity.SecurityIdentity;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.request.EmbeddingCredentialsResolver;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.request.tenant.RequestTenantResolver;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.DatabaseSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared resources for MCP tool providers. Provides command context building and command execution
 * via the existing {@link MeteredCommandProcessor} pipeline.
 */
@ApplicationScoped
public class McpResource {

  private static final Logger LOG = LoggerFactory.getLogger(McpResource.class);

  private final ObjectMapper objectMapper;
  private final MeteredCommandProcessor meteredCommandProcessor;
  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final EmbeddingProviderFactory embeddingProviderFactory;
  private final SchemaCache schemaCache;

  // Per-request dependencies for creating RequestContext in MCP (non-JAX-RS)
  // context
  private final Provider<RoutingContext> routingContextProvider;
  private final Provider<SecurityIdentity> securityIdentityProvider;
  private final Instance<RequestTenantResolver> tenantResolver;
  private final Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver;

  @Inject
  public McpResource(
      ObjectMapper objectMapper,
      MeteredCommandProcessor meteredCommandProcessor,
      MeterRegistry meterRegistry,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CqlSessionCacheSupplier sessionCacheSupplier,
      EmbeddingProviderFactory embeddingProviderFactory,
      RerankingProviderFactory rerankingProviderFactory,
      SchemaCache schemaCache,
      Provider<RoutingContext> routingContextProvider,
      Provider<SecurityIdentity> securityIdentityProvider,
      Instance<RequestTenantResolver> tenantResolver,
      Instance<EmbeddingCredentialsResolver> embeddingCredentialsResolver) {

    this.objectMapper = objectMapper;
    this.meteredCommandProcessor = meteredCommandProcessor;
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.schemaCache = schemaCache;

    this.routingContextProvider = routingContextProvider;
    this.securityIdentityProvider = securityIdentityProvider;
    this.tenantResolver = tenantResolver;
    this.embeddingCredentialsResolver = embeddingCredentialsResolver;

    this.contextBuilderSupplier =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(sessionCacheSupplier.get())
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withEmbeddingProviderFactory(embeddingProviderFactory)
            .withRerankingProviderFactory(rerankingProviderFactory)
            .withMeterRegistry(meterRegistry);
  }

  /**
   * Build a CommandContext for database-level (general) commands. similar to {@link
   * io.stargate.sgv2.jsonapi.api.v1.GeneralResource}
   */
  public CommandContext<?> buildGeneralContext(GeneralCommand command) {
    return contextBuilderSupplier
        .getBuilder(new DatabaseSchemaObject())
        .withCommandName(command.getClass().getSimpleName())
        .withRequestContext(createRequestContext())
        .build();
  }

  /**
   * Process a general or keyspace command and return the CommandResult directly.
   *
   * <p>The Quarkus MCP Server framework automatically encodes the {@link CommandResult} to JSON via
   * its built-in encoder (Jackson), so no manual serialization is needed.
   *
   * @param context The command context (from buildGeneralContext or buildKeyspaceContext)
   * @param command The command to execute
   * @return Uni of CommandResult
   */
  public Uni<CommandResult> processCommand(CommandContext<?> context, Command command) {
    return meteredCommandProcessor.processCommand(context, command);
  }

  /**
   * Create a {@link RequestContext} for the current MCP request. Uses the MCP-specific constructor
   * that resolves the auth token from {@link SecurityIdentity} instead of JAX-RS {@link
   * jakarta.ws.rs.core.SecurityContext}.
   */
  private RequestContext createRequestContext() {
    return new RequestContext(
        routingContextProvider.get(),
        securityIdentityProvider.get(),
        tenantResolver,
        embeddingCredentialsResolver);
  }
}
