package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.Map;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a keyspace.")
@JsonTypeName(CommandName.Names.CREATE_KEYSPACE)
public record CreateKeyspaceCommand(
    @Schema(description = "Required name of the new Keyspace") String name,
    @Nullable @Valid CreateKeyspaceCommand.Options options)
    implements GeneralCommand {

  @Schema(name = "CreateKeyspace.Options", description = "Options for creating a new keyspace.")
  public record Options(@Nullable @Valid Replication replication) {}

  /**
   * Replication options for the create keyspace.
   *
   * @param strategy Cassandra keyspace strategy class name to use (SimpleStrategy or
   *     NetworkTopologyStrategy).
   * @param strategyOptions Options for each strategy. For <code>SimpleStrategy</code>,
   *     `replication_factor` is optional. For the <code>NetworkTopologyStrategy</code> each data
   *     center with replication.
   */
  @Schema(description = "Cassandra based replication settings.")
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

  /** {@inheritDoc} */
  @Override
  public CommandName commandName() {
    return CommandName.CREATE_KEYSPACE;
  }
}
