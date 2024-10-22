package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import java.util.function.Supplier;

/**
 * A page of results for metadata based command, use {@link #builder()} to get a builder to pass to
 * {@link GenericOperation}.
 */
public abstract class MetadataAttemptPage<SchemaT extends SchemaObject>
    extends OperationAttemptPage<SchemaT, MetadataAttempt<SchemaT>> {

  private final boolean showSchema;
  private final CommandStatus statusKey;

  private MetadataAttemptPage(
      OperationAttemptContainer<SchemaT, MetadataAttempt<SchemaT>> attempts,
      CommandResultBuilder resultBuilder,
      boolean showSchema,
      CommandStatus statusKey) {
    super(attempts, resultBuilder);
    this.showSchema = showSchema;
    this.statusKey = statusKey;
  }

  public static <SchemaT extends KeyspaceSchemaObject>
      MetadataAttemptPage.Builder<SchemaT> builder() {
    return new MetadataAttemptPage.Builder<>();
  }

  @Override
  protected void buildCommandResult() {
    addAttemptWarningsToResult();
    addAttemptErrorsToResult();
    var metadataAttempts = attempts.completedAttempts();
    if (metadataAttempts.size() > 1) {
      throw new IllegalArgumentException("Only one attempt is expected for metadata commands");
    }
    if (!metadataAttempts.isEmpty()) {
      if (showSchema) {
        resultBuilder.addStatus(statusKey, attempts.getFirst().getSchema());
      } else {
        resultBuilder.addStatus(statusKey, attempts.getFirst().getNames());
      }
    }
  }

  public static class Builder<SchemaT extends SchemaObject>
      extends OperationAttemptPageBuilder<SchemaT, MetadataAttempt<SchemaT>> {

    private boolean showSchema = false;
    private CommandStatus statusKey;

    Builder() {}

    public Builder<SchemaT> showSchema(boolean showSchema) {
      this.showSchema = showSchema;
      return this;
    }

    public Builder<SchemaT> usingCommandStatus(CommandStatus statusKey) {
      this.statusKey = statusKey;
      return this;
    }

    public Supplier<CommandResult> getOperationPage() {
      return new MetadataAttemptPage<>(
          attempts,
          CommandResult.statusOnlyBuilder(useErrorObjectV2, debugMode),
          showSchema,
          statusKey) {};
    }
  }
}
