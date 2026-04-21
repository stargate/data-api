package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.DeprecatedCommand;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(
    description =
        "Command that creates a namespace. This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead.",
    deprecated = true)
@JsonTypeName(CommandName.Names.CREATE_NAMESPACE)
public record CreateNamespaceCommand(
    @Schema(
            description =
                "Required name of the new Keyspace. This command has been deprecated and will be removed in future releases, use createKeyspace instead.",
            deprecated = true)
        String name,
    @Nullable @Valid CreateNamespaceCommand.Options options)
    implements GeneralCommand, DeprecatedCommand {

  @Schema(
      name = "CreateNamespaceCommand.Options",
      description =
          "Options for creating a new namespace. This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead.",
      deprecated = true)
  public record Options(@Nullable @Valid Replication replication) {}

  /**
   * Replication options for the create namespace.
   *
   * @param strategy Cassandra keyspace strategy class name to use (SimpleStrategy or
   *     NetworkTopologyStrategy).
   * @param strategyOptions Additional strategy options as a flat map. For {@code SimpleStrategy},
   *     use {@code replication_factor} (integer). For {@code NetworkTopologyStrategy}, use
   *     datacenter names as keys with replication factor as values (e.g. {@code "dc1": 3}).
   */
  @Schema(
      description =
          "Cassandra based replication settings. "
              + "For SimpleStrategy, use {\"class\": \"SimpleStrategy\", \"replication_factor\": N}. "
              + "For NetworkTopologyStrategy, use {\"class\": \"NetworkTopologyStrategy\", \"datacenter_name\": N, ...}. "
              + "This createNamespace has been deprecated and will be removed in future releases, use createKeyspace instead.",
      deprecated = true)
  public record Replication(
      @NotNull()
          @Pattern(regexp = "SimpleStrategy|NetworkTopologyStrategy")
          @JsonProperty("class")
          @Schema(
              description =
                  "Cassandra replication strategy class. Must be either 'SimpleStrategy' or 'NetworkTopologyStrategy'.")
          String strategy,
      @JsonAnySetter
          @Schema(
              description =
                  "Additional strategy options as a flat map. "
                      + "For SimpleStrategy, use 'replication_factor' (integer). "
                      + "For NetworkTopologyStrategy, use datacenter names as keys with replication factor as values "
                      + "(e.g. 'dc1': 3, 'dc2': 2).",
              type = SchemaType.OBJECT)
          Map<String, Integer> strategyOptions) {}

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_NAMESPACE;
  }

  /** {@inheritDoc} */
  @Override
  public CommandName useCommandName() {
    return CommandName.CREATE_KEYSPACE;
  }
}
