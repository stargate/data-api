package io.stargate.sgv2.jsonapi.service.operation.tables;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.SchemaAttempt;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOption;
import io.stargate.sgv2.jsonapi.service.operation.query.CQLOptions;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiGeneralIndex;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiVectorIndex;
import java.util.Objects;

/** Builder for a {@link CreateIndexAttempt}. */
public class CreateIndexAttemptBuilder {
  private int position = 0;

  private final TableSchemaObject schemaObject;
  private SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy;
  // must be specified, the default should not be defined in here
  private Boolean ifNotExists = null;

  public CreateIndexAttemptBuilder(TableSchemaObject schemaObject) {
    this.schemaObject = Objects.requireNonNull(schemaObject, "schemaObject object cannot be null");
  }

  public CreateIndexAttemptBuilder withIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return this;
  }

  public CreateIndexAttemptBuilder withSchemaRetryPolicy(
      SchemaAttempt.SchemaRetryPolicy schemaRetryPolicy) {
    this.schemaRetryPolicy = schemaRetryPolicy;
    return this;
  }

  private void checkBuildPreconditions() {
    Objects.requireNonNull(ifNotExists, "ifNotExists cannot be null");
    Objects.requireNonNull(schemaRetryPolicy, "schemaRetryPolicy cannot be null");
  }

  private CQLOptions.CreateIndexStartCQLOptions buildCqlOptions() {
    var cqlOptions = new CQLOptions.CreateIndexStartCQLOptions();
    if (ifNotExists) {
      cqlOptions.addBuilderOption(CQLOption.ForCreateIndexStart.ifNotExists(true));
    }
    return cqlOptions;
  }

  public CreateIndexAttempt build(ApiGeneralIndex apiGeneralIndex) {
    Objects.requireNonNull(apiGeneralIndex, "apiGeneralIndex cannot be null");
    checkBuildPreconditions();

    return new CreateIndexAttempt(
        position++, schemaObject, schemaRetryPolicy, apiGeneralIndex, buildCqlOptions());
  }

  public CreateIndexAttempt build(ApiVectorIndex apiVectorIndex) {
    Objects.requireNonNull(apiVectorIndex, "apiVectorIndex cannot be null");
    checkBuildPreconditions();

    return new CreateIndexAttempt(
        position++, schemaObject, schemaRetryPolicy, apiVectorIndex, buildCqlOptions());
  }
}
