package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropIndexCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DropTableCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.ListTablesCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeature;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.config.feature.FeaturesConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
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

@Path(KeyspaceResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Keyspaces")
public class KeyspaceResource {

  public static final String BASE_PATH = GeneralResource.BASE_PATH + "/{keyspace}";
  private final MeteredCommandProcessor meteredCommandProcessor;

  private final FeaturesConfig apiFeatureConfig;

  private final OperationsConfig operationsConfig;

  @Inject private DataApiRequestInfo dataApiRequestInfo;

  @Inject
  public KeyspaceResource(
      MeteredCommandProcessor meteredCommandProcessor,
      FeaturesConfig apiFeatureConfig,
      OperationsConfig operationsConfig) {
    this.meteredCommandProcessor = meteredCommandProcessor;
    this.apiFeatureConfig = apiFeatureConfig;
    this.operationsConfig = operationsConfig;
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
                        ListTablesCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "createCollection"),
                @ExampleObject(ref = "createCollectionLexical"),
                @ExampleObject(ref = "createCollectionVectorSearch"),
                @ExampleObject(ref = "findCollections"),
                @ExampleObject(ref = "deleteCollection"),
                @ExampleObject(ref = "createTableWithSingleKey"),
                @ExampleObject(ref = "createTableWithMultipleKeys"),
                @ExampleObject(ref = "dropTable"),
                @ExampleObject(ref = "dropIndex"),
                @ExampleObject(ref = "listTables")
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

    final ApiFeatures apiFeatures =
        ApiFeatures.fromConfigAndRequest(apiFeatureConfig, dataApiRequestInfo.getHttpHeaders());

    // create context
    // TODO: Aaron , left here to see what CTOR was used, there was a lot of different ones.
    //    CommandContext commandContext = new CommandContext(keyspace, null);
    // HACK TODO: The above did not set a command name on the command context, how did that work ?
    CommandContext<KeyspaceSchemaObject> commandContext =
        new CommandContext<>(
            new KeyspaceSchemaObject(keyspace),
            null,
            command.getClass().getSimpleName(),
            null,
            apiFeatures,
            operationsConfig);

    // Need context first to check if feature is enabled
    if (command instanceof TableOnlyCommand && !apiFeatures.isFeatureEnabled(ApiFeature.TABLES)) {
      return Uni.createFrom()
          .item(
              new ThrowableCommandResultSupplier(
                  ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED.toApiException()))
          .map(commandResult -> commandResult.toRestResponse());
    }

    // call processor
    return meteredCommandProcessor
        .processCommand(dataApiRequestInfo, commandContext, command)
        // map to 2xx unless overridden by error
        .map(commandResult -> commandResult.toRestResponse());
  }
}
