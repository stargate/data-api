package io.stargate.sgv3.docsapi.operations;

public class SchemaChangeResult {
  public final boolean schemaChanged;

  private SchemaChangeResult(boolean schemaChanged) {
    this.schemaChanged = schemaChanged;
  }

  public static SchemaChangeResult from(boolean schemaChanged) {
    return new SchemaChangeResult(schemaChanged);
  }

  public OperationResult createOperationResult() {
    return OperationResult.builder().withSchemaChange(schemaChanged).build();
  }
}
