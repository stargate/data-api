package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import java.util.Map;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a namespace (database).")
@JsonTypeName("createDatabase")
public record CreateDatabaseCommand(
    @NotBlank(message = "Database name must be specified.")
        @Size(max = 48, message = "Database name must have maximum of 48 characters.")
        @Schema(description = "Name of the database")
        String name,
    @Nullable @Valid CreateDatabaseCommand.Options options)
    implements GeneralCommand {

  @Schema(
      name = "CreateDatabaseCommand.Options",
      description = "Options for creating a new database.")
  public record Options(@Valid Replication replication) {}

  /**
   * Replication options for the create namespace.
   *
   * @param strategy Cassandra keyspace strategy class name to use (SimpleStrategy or
   *     NetworkTopologyStrategy).
   * @param strategyOptions Options for each strategy. For <code>SimpleStrategy</code>,
   *     `replication_factor` is optional. For the <code>NetworkTopologyStrategy</code> each data
   *     center with replication.
   */
  @Schema(description = "Cassandra based replication settings.")
  // no record due to the @JsonAnySetter
  public static class Replication {
    @NotNull
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
}
