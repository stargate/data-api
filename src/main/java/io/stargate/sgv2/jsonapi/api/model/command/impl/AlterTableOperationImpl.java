package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.ColumnsDescContainer;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Each operation is represented by a record that implements this interface. */
public class AlterTableOperationImpl {

  @Schema(description = "Operation to add columns to a table.")
  @JsonTypeName("add")
  public record AddColumns(ColumnsDescContainer columns) implements AlterTableOperation {}

  @Schema(description = "Operation to drop columns of a table.")
  @JsonTypeName("drop")
  public record DropColumns(List<String> columns) implements AlterTableOperation {}

  @Schema(description = "Operation to add vectorize service to column definition.")
  @JsonTypeName("addVectorize")
  public record AddVectorize(Map<String, VectorizeConfig> columns) implements AlterTableOperation {}

  @Schema(description = "Operation to drop vectorize service definition for columns.")
  @JsonTypeName("dropVectorize")
  public record DropVectorize(List<String> columns) implements AlterTableOperation {}
}
