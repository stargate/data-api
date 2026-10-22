package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.TextContent;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.api.model.command.tracing.RequestTracing;
import jakarta.ws.rs.core.Response;
import java.util.*;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.media.SchemaProperty;
import org.jboss.resteasy.reactive.RestResponse;

/**
 * Use the {@link CommandResultBuilder} to create a {@link CommandResult} for a command response,
 * for creation see {@link #singleDocumentBuilder} and the other factory methods.
 *
 * <p>Comments on {@link CommandResultBuilder} explain future work here.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CommandResult(
    @Schema(
            description =
                "A response data holding documents that were returned as the result of a command.",
            nullable = true,
            oneOf = {ResponseData.MultiResponseData.class, ResponseData.SingleResponseData.class})
        ResponseData data,
    // **
    @Schema(
            description =
                "Status objects, generally describe the side effects of commands, such as the number of updated or inserted documents.",
            nullable = true,
            minProperties = 1,
            properties = {
              @SchemaProperty(
                  name = "insertedIds",
                  description = "IDs of inserted documents for an insert command.",
                  type = SchemaType.ARRAY,
                  implementation = String.class,
                  nullable = true)
            })
        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        Map<CommandStatus, Object> status,
    // **
    @JsonInclude(JsonInclude.Include.NON_EMPTY) @Schema(nullable = true)
        List<CommandError> errors) {

  public CommandResult {

    // make both of these not null, so they can be mutated if needed
    // the decorators on the record fields tell Jackson to exclude when they are null or empty
    if (null == status) {
      status = new HashMap<>();
    }
    if (null == errors) {
      errors = new ArrayList<>();
    }
  }

  /**
   * Get a builder for the {@link CommandResult} for a single document response, see {@link
   * CommandResultBuilder}
   */
  public static CommandResultBuilder singleDocumentBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(
        CommandResultBuilder.ResponseType.SINGLE_DOCUMENT, requestTracing);
  }

  /** See {@link #singleDocumentBuilder(RequestTracing)} */
  public static CommandResultBuilder multiDocumentBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(
        CommandResultBuilder.ResponseType.MULTI_DOCUMENT, requestTracing);
  }

  /** See {@link #singleDocumentBuilder(RequestTracing)} */
  public static CommandResultBuilder statusOnlyBuilder(RequestTracing requestTracing) {
    return new CommandResultBuilder(CommandResultBuilder.ResponseType.STATUS_ONLY, requestTracing);
  }

  /**
   * Create the {@link RestResponse} based on the state of this CommandResult.
   *
   * <p>The HTTP response will be 200, unless there is a single error in the errors list with a
   * non-200 status, in which case the first of those will be used for the response status code.
   */
  public RestResponse<CommandResult> toRestResponse() {

    Optional<RestResponse<CommandResult>> maybeErrorResponse =
        errors == null
            ? Optional.empty()
            : errors().stream()
                .filter(error -> error.httpStatus() != Response.Status.OK)
                .findFirst()
                .map(
                    firstError ->
                        RestResponse.ResponseBuilder.create(firstError.httpStatus(), this).build());

    return maybeErrorResponse.orElseGet(() -> RestResponse.ok(this));
  }

  /**
   * Create a {@link ToolResponse} from this {@link CommandResult} for MCP tool responses.
   *
   * <p>Text content contains the complete Data API response. Structured content and status metadata
   * retain their existing shapes for clients that consume them.
   *
   * @param objectMapper the configured Data API response mapper
   * @return A new {@link ToolResponse} representing this command result.
   */
  public ToolResponse toToolResponse(ObjectMapper objectMapper) {

    final String text;
    try {
      text = objectMapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      // Neither the partial response nor the exception message is safe to return here.
      return ToolResponse.error("Unable to serialize the command result.");
    }

    boolean hasErrors = errors != null && !errors.isEmpty();

    // Map "status" in CommandResult to _meta in ToolResponse
    Map<MetaKey, Object> meta =
        (status != null && !status.isEmpty()) ? Map.of(MetaKey.of("status"), status) : Map.of();

    return new ToolResponse(
        hasErrors,
        List.of(new TextContent(text)),
        hasErrors ? Map.of("errors", errors) : data,
        meta);
  }

  /**
   * Adds a warning to this CommandResult's status map.
   *
   * @param warning message
   */
  public void addWarning(CommandError warning) {
    List<CommandError> warnings =
        (List<CommandError>) status.computeIfAbsent(CommandStatus.WARNINGS, k -> new ArrayList<>());
    warnings.add(warning);
  }
}
