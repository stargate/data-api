package io.stargate.sgv2.jsonapi.api.v1;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.ConfigPreLoader;
import io.stargate.sgv2.jsonapi.api.model.command.*;
import io.stargate.sgv2.jsonapi.api.model.command.impl.AlterTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateVectorIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EstimatedDocumentCountCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListIndexesCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorColumnDefinition;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProvider;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingProviderFactory;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import io.stargate.sgv2.jsonapi.service.reranking.operation.RerankingProviderFactory;
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

@Path(CollectionResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Documents")
public class CollectionResource {

  public static final String BASE_PATH = GeneralResource.BASE_PATH + "/{keyspace}/{collection}";

  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject private SchemaCache schemaCache;

  private EmbeddingProviderFactory embeddingProviderFactory;

  @Inject private RequestContext requestContext;

  //  need to keep for a little because we have to check the schema type before making the command
  // context
  // TODO remove apiFeatureConfig as a property after cleanup for how we get schema from cache
  @Inject private FeaturesConfig apiFeatureConfig;

  private final CommandContext.BuilderSupplier contextBuilderSupplier;

  @Inject
  public CollectionResource(
      MeteredCommandProcessor meteredCommandProcessor,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter,
      CQLSessionCache cqlSessionCache,
      EmbeddingProviderFactory embeddingProviderFactory,
      RerankingProviderFactory rerankingProviderFactory) {
    this.embeddingProviderFactory = embeddingProviderFactory;
    this.meteredCommandProcessor = meteredCommandProcessor;

    contextBuilderSupplier =
        CommandContext.builderSupplier()
            .withJsonProcessingMetricsReporter(jsonProcessingMetricsReporter)
            .withCqlSessionCache(cqlSessionCache)
            .withCommandConfig(ConfigPreLoader.getPreLoadOrEmpty())
            .withEmbeddingProviderFactory(embeddingProviderFactory)
            .withRerankingProviderFactory(rerankingProviderFactory);
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(
      value = {
        @Parameter(name = "keyspace", ref = "keyspace"),
        @Parameter(name = "collection", ref = "collection")
      })
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        CountDocumentsCommand.class,
                        DeleteOneCommand.class,
                        DeleteManyCommand.class,
                        FindOneCommand.class,
                        FindCommand.class,
                        FindOneAndDeleteCommand.class,
                        FindOneAndReplaceCommand.class,
                        FindOneAndUpdateCommand.class,
                        EstimatedDocumentCountCommand.class,
                        InsertOneCommand.class,
                        InsertManyCommand.class,
                        UpdateManyCommand.class,
                        UpdateOneCommand.class,
                        // Table Only commands
                        AlterTableCommand.class,
                        CreateIndexCommand.class,
                        CreateVectorIndexCommand.class,
                        ListIndexesCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "countDocuments"),
                @ExampleObject(ref = "deleteOne"),
                @ExampleObject(ref = "deleteMany"),
                @ExampleObject(ref = "find"),
                @ExampleObject(ref = "findVectorSearch"),
                @ExampleObject(ref = "findOne"),
                @ExampleObject(ref = "findOneVectorSearch"),
                @ExampleObject(ref = "findOneAndDelete"),
                @ExampleObject(ref = "findOneAndReplace"),
                @ExampleObject(ref = "findOneAndUpdate"),
                @ExampleObject(ref = "estimatedDocumentCount"),
                @ExampleObject(ref = "insertOne"),
                @ExampleObject(ref = "insertMany"),
                @ExampleObject(ref = "updateMany"),
                @ExampleObject(ref = "updateOne"),
                @ExampleObject(ref = "alterTableAddColumns"),
                @ExampleObject(ref = "alterTableDropColumns"),
                @ExampleObject(ref = "alterTableAddVectorize"),
                @ExampleObject(ref = "alterTableDropVectorize"),
                @ExampleObject(ref = "createIndex"),
                @ExampleObject(ref = "createVectorIndex"),
                @ExampleObject(ref = "listIndexes"),
                @ExampleObject(ref = "insertOneTables"),
                @ExampleObject(ref = "insertManyTables"),
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
                    @ExampleObject(ref = "resultCount"),
                    @ExampleObject(ref = "resultDeleteOne"),
                    @ExampleObject(ref = "resultDeleteMany"),
                    @ExampleObject(ref = "resultFind"),
                    @ExampleObject(ref = "resultFindOne"),
                    @ExampleObject(ref = "resultFindOneAndDelete"),
                    @ExampleObject(ref = "resultFindOneAndReplace"),
                    @ExampleObject(ref = "resultFindOneAndUpdate"),
                    @ExampleObject(ref = "resultFindOneAndUpdateUpsert"),
                    @ExampleObject(ref = "resultInsert"),
                    @ExampleObject(ref = "resultUpdateOne"),
                    @ExampleObject(ref = "resultUpdateOneUpsert"),
                    @ExampleObject(ref = "resultUpdateMany"),
                    @ExampleObject(ref = "resultUpdateManyUpsert"),
                    @ExampleObject(ref = "resultError"),
                    @ExampleObject(ref = "resultDdl"),
                    @ExampleObject(ref = "resultListIndexes"),
                    @ExampleObject(ref = "insertManyTablesResponse"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid CollectionCommand command,
      @PathParam("keyspace") @NotEmpty String keyspace,
      @PathParam("collection") @NotEmpty String collection) {
    return schemaCache
        .getSchemaObject(
            requestContext,
            requestContext.getTenantId(),
            keyspace,
            collection,
            CommandType.DDL.equals(command.commandName().getCommandType()))
        .onItemOrFailure()
        .transformToUni(
            (schemaObject, throwable) -> {
              if (throwable != null) {

                // We failed to get the schema object, or failed to build it.
                Throwable error = throwable;
                if (throwable instanceof RuntimeException && throwable.getCause() != null) {
                  error = throwable.getCause();
                } else if (error instanceof JsonApiException jsonApiException) {
                  return Uni.createFrom().failure(jsonApiException);
                }
                // otherwise use generic for now
                return Uni.createFrom().item(new ThrowableCommandResultSupplier(error));

              } else {

                // TODO No need for the else clause here, simplify
                var apiFeatures =
                    ApiFeatures.fromConfigAndRequest(
                        apiFeatureConfig, requestContext.getHttpHeaders());
                if ((schemaObject.type() == SchemaObject.SchemaObjectType.TABLE)
                    && !apiFeatures.isFeatureEnabled(ApiFeature.TABLES)) {
                  return Uni.createFrom()
                      .failure(ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED.toApiException());
                }

                // TODO: This needs to change, currently it is only checking if there is vectorize
                // for the $vector column in a collection

                VectorColumnDefinition vectorColDef = null;
                if (schemaObject.type() == SchemaObject.SchemaObjectType.COLLECTION) {
                  vectorColDef =
                      schemaObject
                          .vectorConfig()
                          .getColumnDefinition(VECTOR_EMBEDDING_TEXT_FIELD)
                          .orElse(null);
                } else if (schemaObject.type() == SchemaObject.SchemaObjectType.TABLE) {
                  vectorColDef =
                      schemaObject
                          .vectorConfig()
                          .getFirstVectorColumnWithVectorizeDefinition()
                          .orElse(null);
                }
                EmbeddingProvider embeddingProvider =
                    (vectorColDef == null || vectorColDef.vectorizeDefinition() == null)
                        ? null
                        : embeddingProviderFactory.getConfiguration(
                            requestContext.getTenantId(),
                            requestContext.getCassandraToken(),
                            vectorColDef.vectorizeDefinition().provider(),
                            vectorColDef.vectorizeDefinition().modelName(),
                            vectorColDef.vectorSize(),
                            vectorColDef.vectorizeDefinition().parameters(),
                            vectorColDef.vectorizeDefinition().authentication(),
                            command.getClass().getSimpleName());

                var commandContext =
                    contextBuilderSupplier
                        .getBuilder(schemaObject)
                        .withEmbeddingProvider(embeddingProvider)
                        .withCommandName(command.getClass().getSimpleName())
                        .withRequestContext(requestContext)
                        .build();

                return meteredCommandProcessor.processCommand(commandContext, command);
              }
            })
        .map(commandResult -> commandResult.toRestResponse());
  }
}
