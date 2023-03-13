package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
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

  public static final String BASE_PATH = "/v1/{namespace}/{collection}";

  private final CommandProcessor commandProcessor;

  @Inject
  public CollectionResource(CommandProcessor commandProcessor) {
    this.commandProcessor = commandProcessor;
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = "namespace"),
        @Parameter(name = "collection", ref = "collection")
      })
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        CountDocumentsCommands.class,
                        DeleteOneCommand.class,
                        DeleteManyCommand.class,
                        FindOneCommand.class,
                        FindCommand.class,
                        FindOneAndUpdateCommand.class,
                        InsertOneCommand.class,
                        InsertManyCommand.class,
                        UpdateManyCommand.class,
                        UpdateOneCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "countDocuments"),
                @ExampleObject(ref = "deleteOne"),
                @ExampleObject(ref = "deleteMany"),
                @ExampleObject(ref = "findOne"),
                @ExampleObject(ref = "find"),
                @ExampleObject(ref = "findOneAndUpdate"),
                @ExampleObject(ref = "insertOne"),
                @ExampleObject(ref = "insertMany"),
                @ExampleObject(ref = "updateMany"),
                @ExampleObject(ref = "updateOne"),
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
                    @ExampleObject(ref = "resultRead"),
                    @ExampleObject(ref = "resultFindOneAndUpdate"),
                    @ExampleObject(ref = "resultInsert"),
                    @ExampleObject(ref = "resultUpdateOne"),
                    @ExampleObject(ref = "resultUpdateOneUpsert"),
                    @ExampleObject(ref = "resultUpdateMany"),
                    @ExampleObject(ref = "resultUpdateManyUpsert"),
                    @ExampleObject(ref = "resultError"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid CollectionCommand command,
      @PathParam("namespace") String namespace,
      @PathParam("collection") String collection) {

    // create context
    CommandContext commandContext = new CommandContext(namespace, collection);

    // call processor
    return commandProcessor
        .processCommand(commandContext, command)

        // map to 2xx always
        .map(RestResponse::ok);
  }
}
