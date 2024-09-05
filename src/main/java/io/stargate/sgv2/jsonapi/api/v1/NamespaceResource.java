package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.TableOnlyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteCollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCollectionsCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.ApiTablesConfig;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
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

@Path(NamespaceResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Namespaces")
public class NamespaceResource {

  public static final String BASE_PATH = "/v1/{namespace}";
  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject private DataApiRequestInfo dataApiRequestInfo;

  @Inject ApiTablesConfig apiTablesConfig;

  @Inject
  public NamespaceResource(MeteredCommandProcessor meteredCommandProcessor) {
    this.meteredCommandProcessor = meteredCommandProcessor;
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(value = {@Parameter(name = "namespace", ref = "namespace")})
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        CreateCollectionCommand.class,
                        FindCollectionsCommand.class,
                        DeleteCollectionCommand.class
                        // TODO, hide table feature detail before it goes public,
                        // https://github.com/stargate/data-api/pull/1360
                        //                        CreateTableCommand.class,
                        //                        DropTableCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "createCollection"),
                @ExampleObject(ref = "createCollectionVectorSearch"),
                @ExampleObject(ref = "findCollections"),
                @ExampleObject(ref = "deleteCollection"),
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
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid NamespaceCommand command,
      @PathParam("namespace")
          @NotNull
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Size(min = 1, max = 48)
          String namespace) {

    if (command instanceof TableOnlyCommand && !apiTablesConfig.enabled()) {
      return Uni.createFrom()
          .item(
              new ThrowableCommandResultSupplier(
                  ErrorCodeV1.TABLE_FEATURE_NOT_ENABLED.toApiException()))
          .map(commandResult -> commandResult.toRestResponse());
    }

    // create context
    // TODO: Aaron , left here to see what CTOR was used, there was a lot of different ones.
    //    CommandContext commandContext = new CommandContext(namespace, null);
    // HACK TODO: The above did not set a command name on the command context, how did that work ?
    CommandContext<KeyspaceSchemaObject> commandContext =
        new CommandContext<>(new KeyspaceSchemaObject(namespace), null, "", null);

    //     call processor
    return meteredCommandProcessor
        .processCommand(dataApiRequestInfo, commandContext, command)
        // map to 2xx unless overridden by error
        .map(commandResult -> commandResult.toRestResponse());
  }
}
