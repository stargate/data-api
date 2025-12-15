package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.micrometer.core.instrument.MeterRegistry;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.CqlSessionCacheSupplier;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectCacheSupplier;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(KeyspaceResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Keyspaces")
public class KeyspaceResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyspaceResource.class);

  public static final String BASE_PATH = GeneralResource.BASE_PATH + "/{keyspace}";

  @Inject private RequestContext requestContext;

  private final SchemaObjectCacheSupplier schemaObjectCacheSupplier;
  private final CommandContext.BuilderSupplier contextBuilderSupplier;
  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject
  public KeyspaceResource(
      SchemaObjectCacheSupplier schemaObjectCacheSupplier,
      MeteredCommandProcessor meteredCommandProcessor,
      MeterRegistry meterRegistry,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CqlSessionCacheSupplier sessionCacheSupplier,
      EmbeddingProviderFactory embeddingProviderFactory,
      RerankingProviderFactory rerankingProviderFactory) {

    this.schemaObjectCacheSupplier = schemaObjectCacheSupplier;
    this.meteredCommandProcessor = meteredCommandProcessor;

    contextBuilderSupplier =
        CommandContext.builderSupplier()
            // old code did not pass a jsonProcessingMetricsReporter not sure why - Aaron Feb 10
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(sessionCacheSupplier.get())
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withEmbeddingProviderFactory(embeddingProviderFactory)
            .withRerankingProviderFactory(rerankingProviderFactory)
            .withMeterRegistry(meterRegistry);
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(value = {@Parameter(name = "keyspace", ref = "keyspace")})
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        CreateCollectionCommand.class,
                        FindCollectionsCommand.class,
                        DeleteCollectionCommand.class,
                        // Table only commands
                        CreateTableCommand.class,
                        DropIndexCommand.class,
                        DropTableCommand.class,
                        ListTablesCommand.class,
                        ListTypesCommand.class,
                        CreateTypeCommand.class,
                        AlterTypeCommand.class,
                        DropTypeCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "createCollection"),
                @ExampleObject(ref = "createCollectionLexical"),
                @ExampleObject(ref = "createCollectionReranking"),
                @ExampleObject(ref = "createCollectionVectorSearch"),
                @ExampleObject(ref = "findCollections"),
                @ExampleObject(ref = "deleteCollection"),
                @ExampleObject(ref = "createTableWithSingleKey"),
                @ExampleObject(ref = "createTableWithMultipleKeys"),
                @ExampleObject(ref = "dropTable"),
                @ExampleObject(ref = "dropIndex"),
                @ExampleObject(ref = "listTables"),
                @ExampleObject(ref = "listTypes"),
                @ExampleObject(ref = "createType"),
                @ExampleObject(ref = "alterType"),
                @ExampleObject(ref = "dropType")
              }))
  @APIResponses(
      @APIResponse(
          responseCode = "200",
          description =
              "Call successful. Returns result of the command execution. Note that in case of errors, response code remains `HTTP 200`.",
          content =
              @Content(
                  mediaType = MediaType.APPLICATION_JSON,
                  schema = @Schema(implementation = CommandResult.class),
                  examples = {
                    @ExampleObject(ref = "resultCreate"),
                    @ExampleObject(ref = "resultFindCollections"),
                    @ExampleObject(ref = "resultError"),
                    @ExampleObject(ref = "resultDdl"),
                    @ExampleObject(ref = "listTablesResponse"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid KeyspaceCommand command, @PathParam("keyspace") @NotEmpty String keyspace) {

    // create context
    // TODO: Aaron , left here to see what CTOR was used, there was a lot of different ones.
    //    CommandContext commandContext = new CommandContext(keyspace, null);
    // HACK TODO: The above did not set a command name on the command context, how did that work ?

    var keyspaceIdentifier =
        SchemaObjectIdentifier.forKeyspace(
            requestContext.tenant(), cqlIdentifierFromUserInput(keyspace));

    // Force refresh on all keyspace commands because they are all DDL commands
    return schemaObjectCacheSupplier
        .get()
        .getKeyspace(requestContext, keyspaceIdentifier, requestContext.userAgent(), true)
        .flatMap(
            keyspaceSchemaObject -> {
              var commandContext =
                  contextBuilderSupplier
                      .getBuilder(keyspaceSchemaObject)
                      .withEmbeddingProvider(null)
                      .withCommandName(command.getClass().getSimpleName())
                      .withRequestContext(requestContext)
                      .build();

              // Need context first to check if feature is enabled, because of request overrides
              if (command instanceof TableOnlyCommand
                  && !commandContext.apiFeatures().isFeatureEnabled(ApiFeature.TABLES)) {
                return Uni.createFrom()
                    .item(
                        new ThrowableCommandResultSupplier(
                            ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED.toApiException()))
                    .map(commandResult -> commandResult.toRestResponse());
              }

              // call processor
              return meteredCommandProcessor
                  .processCommand(commandContext, command)
                  // map to 2xx unless overridden by error
                  .map(commandResult -> commandResult.toRestResponse())
                  .onTermination()
                  .invoke(
                      () -> {
                        try {
                          commandContext.close();
                        } catch (Exception e) {
                          LOGGER.error(
                              "Error closing the command context for requestContext={}",
                              requestContext,
                              e);
                        }
                      });
            });
  }
}
