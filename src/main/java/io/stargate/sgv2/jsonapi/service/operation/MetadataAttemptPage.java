package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/**
 * A page of results from a list table command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public abstract class MetadataAttemptPage<SchemaT extends SchemaObject>
    extends OperationAttemptPage<SchemaT, MetadataAttempt<SchemaT>> {

  private final boolean showSchema;

  private MetadataAttemptPage(
      OperationAttemptContainer<SchemaT, MetadataAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      boolean showSchema) {
    super(attempts, resultBuilder);
    this.showSchema = showSchema;
  }

  public static <SchemaT extends KeyspaceSchemaObject>
      MetadataAttemptPage.Builder<SchemaT> builder() {
    return new MetadataAttemptPage.Builder<>();
  }

  @Override
  protected void buildCommandResult() {
    addAttemptWarningsToResult();
    // nor now its onlt ListTableAttempt we can cast it
    ListTablesAttempt response = (ListTablesAttempt) attempts.get(0);
    if (showSchema) {
      resultBuilder.addStatus(CommandStatus.EXISTING_TABLES, response.getTablesSchema());

    } else {
      resultBuilder.addStatus(CommandStatus.EXISTING_TABLES, response.getTableNames());
    }
  }

  public static class Builder<SchemaT extends SchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, MetadataAttempt<SchemaT>> {

    private boolean showSchema = false;

    Builder() {}

    public Builder<SchemaT> showSchema(boolean showSchema) {
      this.showSchema = showSchema;
      return this;
    }

    public Supplier<CommandResult> getOperationPage() {
      var resultBuilder =
          new CommandResultBuilder(
              CommandResultBuilder.ResponseType.STATUS_ONLY, useErrorObjectV2, debugMode);

      return new MetadataAttemptPage<>(attempts, resultBuilder, showSchema) {};
    }
  }
}
