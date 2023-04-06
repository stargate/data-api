package io.stargate.sgv2.jsonapi.service.schema;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.api.common.schema.SchemaManager;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.util.function.Function;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class CollectionManager {

  // missing keyspace function
  private static final Function<String, Uni<? extends Schema.CqlKeyspaceDescribe>>
      MISSING_KEYSPACE_FUNCTION =
          keyspace -> {
            String message = "Unknown namespace %s, you must create it first.".formatted(keyspace);
            Exception exception = new JsonApiException(ErrorCode.NAMESPACE_DOES_NOT_EXIST, message);
            return Uni.createFrom().failure(exception);
          };
  private final SchemaManager schemaManager;

  private final JsonapiTableMatcher tableMatcher;

  @Inject
  public CollectionManager(SchemaManager schemaManager) {
    this(schemaManager, new JsonapiTableMatcher());
  }

  public CollectionManager(SchemaManager schemaManager, JsonapiTableMatcher tableMatcher) {
    this.schemaManager = schemaManager;
    this.tableMatcher = tableMatcher;
  }

  /**
   * Returns all valid collection tables from a given keyspace.
   *
   * <p>Emits a failure in case:
   *
   * <ol>
   *   <li>Keyspace does not exists, with {@link ErrorCode#NAMESPACE_DOES_NOT_EXIST}
   * </ol>
   *
   * @param namespace Namespace.
   * @return Multi of {@link io.stargate.bridge.proto.Schema.CqlTable}s.
   */
  public Multi<Schema.CqlTable> getValidCollectionTables(String namespace) {
    // get all tables
    return schemaManager
        .getTables(namespace, MISSING_KEYSPACE_FUNCTION)

        // filter for valid collections
        .filter(tableMatcher);
  }
}
