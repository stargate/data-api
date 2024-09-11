package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Command {@link CreateNamespaceCommand} is deprecated, please switch to {@link
 * CreateKeyspaceCommand} Support it for backward-compatibility
 */
@Schema(
    description =
        "Command that creates a namespace. This command has been deprecated and will be removed in future releases, use CreateKeyspaceCommand instead.",
    deprecated = true)
@JsonTypeName("createNamespace")
public record CreateNamespaceCommand(
    @NotNull
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Size(min = 1, max = 48)
        @Schema(description = "Name of the namespace")
        String name,
    @Nullable @Valid CreateNamespaceCommand.Options options)
    implements GeneralCommand, DeprecatedCommand {

  @Schema(
      name = "CreateNamespaceCommand.Options",
      description =
          "Options for creating a new namespace. This command has been deprecated and will be removed in future releases, use CreateKeyspaceCommand instead.",
      deprecated = true)
  public record Options(@Nullable @Valid Replication replication) {}

  /**
   * Replication options for the create namespace.
   *
   * @param strategy Cassandra keyspace strategy class name to use (SimpleStrategy or
   *     NetworkTopologyStrategy).
   * @param strategyOptions Options for each strategy. For <code>SimpleStrategy</code>,
   *     `replication_factor` is optional. For the <code>NetworkTopologyStrategy</code> each data
   *     center with replication.
   */
  @Schema(
      description =
          "Cassandra based replication settings. This command has been deprecated and will be removed in future releases, use CreateKeyspaceCommand instead.",
      deprecated = true)
  // no record due to the @JsonAnySetter, see
  // https://github.com/FasterXML/jackson-databind/issues/562
  public static class Replication {
    @NotNull()
    @Pattern(regexp = "SimpleStrategy|NetworkTopologyStrategy")
    @JsonProperty("class")
    private String strategy;

    @JsonAnySetter
    @Schema(hidden = true)
    private Map<String, Integer> strategyOptions;

    public String strategy() {
      return strategy;
    }

    public Map<String, Integer> strategyOptions() {
      return strategyOptions;
    }
  }

  /**
   * Override Command interface, this method return the class name of implementation class
   *
   * @return String
   */
  @Override
  public String commandName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Override DeprecatedCommand interface, this method return the class name of corresponding
   * supported command
   *
   * @return String
   */
  @Override
  public String useCommandName() {
    return CreateKeyspaceCommand.class.getSimpleName();
  }

  /**
   * Override DeprecatedCommand interface, get the deprecation message for this implementation
   * command
   *
   * @return String
   */
  @Override
  public String getDeprecationMessage() {
    return String.format(
        "This %s has been deprecated and will be removed in future releases, use %s instead.",
        commandName(), useCommandName());
  }
}
