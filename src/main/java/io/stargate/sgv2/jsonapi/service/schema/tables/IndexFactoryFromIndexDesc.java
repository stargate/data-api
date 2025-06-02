package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserIndexException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for Factories that can create a {@link ApiIndexDef} subclass from the user description in a
 * command.
 *
 * @param <ApiT> The type of the index to create.
 * @param <DescT> The type of the index description.
 */
public abstract class IndexFactoryFromIndexDesc<
        ApiT extends ApiIndexDef, DescT extends IndexDefinitionDesc>
    extends FactoryFromDesc {
  private static final Logger LOGGER = LoggerFactory.getLogger(IndexFactoryFromIndexDesc.class);

  /**
   * Called to create an index from the user description in a command
   *
   * @param apiTableDef
   * @param name
   * @param indexDesc
   * @return
   * @throws UnsupportedUserIndexException The factory should throw this if the user description
   *     cannot be supported.
   */
  public abstract ApiT create(
      TableSchemaObject tableSchemaObject, String indexName, DescT indexDesc);

  /**
   * Checks the target column exits in the table schema, throws {@link
   * SchemaException.Code#UNKNOWN_INDEX_COLUMN} is missing.
   *
   * @param tableSchemaObject The table schema object to check against
   * @param targetIdentifier The CqlIdentifier for the target column
   * @return The {@link ApiColumnDef} for the target column for further checking
   */
  protected ApiColumnDef checkIndexColumnExists(
      TableSchemaObject tableSchemaObject, CqlIdentifier targetIdentifier) {

    var apiColumnDef = tableSchemaObject.apiTableDef().allColumns().get(targetIdentifier);
    if (apiColumnDef == null) {
      throw SchemaException.Code.UNKNOWN_INDEX_COLUMN.get(
          errVars(
              tableSchemaObject,
              map -> {
                map.put(
                    "allColumns", errFmtApiColumnDef(tableSchemaObject.apiTableDef().allColumns()));
                map.put("unknownColumns", errFmt(targetIdentifier));
              }));
    }
    return apiColumnDef;
  }
}
