package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.KeyspaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.column.definition.ColumnDefinition;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

@Schema(description = "Command that creates a api table.")
@JsonTypeName("createTable")
public record CreateTableCommand(
    @NotNull
        @Size(min = 1, max = 48)
        @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
        @Schema(description = "Name of the table")
        String name,
    @Valid @NotNull @Schema(description = "Table definition") Definition definition)
    implements KeyspaceCommand {
  public record Definition(
      @Valid
          @Schema(description = "API table columns definitions", type = SchemaType.OBJECT)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Map<String, ColumnDefinition> columns,
      @Valid
          @Schema(
              description = "Authentication config for chosen embedding service",
              type = SchemaType.OBJECT)
          @JsonInclude(JsonInclude.Include.NON_NULL)
          Partitioning partitioning) {

    public record Partitioning(
        @NotNull
            @Schema(description = "Columns that make the partition keys", type = SchemaType.ARRAY)
            String[] keys,
        @Nullable
            @Schema(description = "Columns that make the ordering keys", type = SchemaType.ARRAY)
            OrderingKey[] orderingKeys) {

      public record OrderingKey(
          @NotNull @Schema(description = "Ordering key column name") String column,
          @Schema(description = "Ordering for the columns", type = SchemaType.BOOLEAN)
              Order order) {
        public enum Order {
          @JsonProperty("ASC")
          ASC,
          @JsonProperty("DESC")
          DESC
        }
      }
    }
  }
}
