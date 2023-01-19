package io.stargate.sgv3.docsapi.api.v3;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import io.stargate.sgv3.docsapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv3.docsapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv3.docsapi.config.constants.OpenApiConstants;
import io.stargate.sgv3.docsapi.service.processor.CommandProcessor;
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
@Tag(name = "Documents", description = "Executes document commands against a single collection.")
public class CollectionResource {

  public static final String BASE_PATH = "/v3/{database}/{collection}";

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
        @Parameter(name = "database", ref = "database"),
        @Parameter(name = "collection", ref = "collection")
      })
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        DeleteOneCommand.class,
                        FindOneCommand.class,
                        FindCommand.class,
                        InsertOneCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "findOne"),
                @ExampleObject(ref = "insertOne"),
                @ExampleObject(ref = "deleteOne"),
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
                    @ExampleObject(ref = "resultRead"),
                    @ExampleObject(ref = "resultInsert"),
                    @ExampleObject(ref = "resultError"),
                    @ExampleObject(ref = "resultDelete"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid Command command,
      @PathParam("database") String database,
      @PathParam("collection") String collection) {

    // create context
    CommandContext commandContext = new CommandContext(database, collection);

    // call processor
    return commandProcessor
        .processCommand(commandContext, command)

        // map to 2xx always
        .map(RestResponse::ok);
  }
}
