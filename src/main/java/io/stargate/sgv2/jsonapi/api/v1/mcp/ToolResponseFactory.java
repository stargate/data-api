package io.stargate.sgv2.jsonapi.api.v1.mcp;

import io.quarkiverse.mcp.server.MetaKey;
import io.quarkiverse.mcp.server.ToolResponse;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Factory that creates a {@link ToolResponse} from a {@link CommandResult}. */
public class ToolResponseFactory {

  private ToolResponseFactory() {}

  /**
   * Create a {@link ToolResponse} from a {@link CommandResult}.
   *
   * <p>Mapping rules:
   *
   * <ul>
   *   <li>{@link CommandResult#errors()} → {@link ToolResponse#isError()} and error content in
   *       {@link ToolResponse#structuredContent()}
   *   <li>{@link CommandResult#data()} → {@link ToolResponse#structuredContent()} (when no errors)
   *   <li>{@link CommandResult#status()} → {@link ToolResponse#_meta()} with key {@code "status"}
   * </ul>
   *
   * @param result The command result to convert. Must not be null.
   * @return A new {@link ToolResponse} representing the command result.
   */
  public static ToolResponse create(CommandResult result) {
    Objects.requireNonNull(result, "Command result must not be null");

    boolean hasErrors = result.errors() != null && !result.errors().isEmpty();

    // Map "status" in CommandResult to _meta in ToolResponse
    Map<MetaKey, Object> meta =
        (result.status() != null && !result.status().isEmpty())
            ? Map.of(MetaKey.of("status"), result.status())
            : Map.of();

    // Map "errors" or "data" to structuredContent
    // Also, structuredContent is expected to be a Record (a plain JSON object {})
    return new ToolResponse(
        hasErrors, List.of(), hasErrors ? Map.of("errors", result.errors()) : result.data(), meta);
  }
}
