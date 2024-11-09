package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDesc;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedUserIndexException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base for Factories that can create a {@link ApiIndexDef} subclass from the user description in a
 * command.
 *
 * @param <ApiT> The type of the index to create.
 * @param <DescT> The type of the index description.
 */
public abstract class IndexFactoryFromIndexDesc<ApiT extends ApiIndexDef, DescT extends IndexDesc>
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
   * Called to create an index from the user description in a command that we know we do not
   * support.
   */
  public UnsupportedIndex createUnsupported(String name, DescT indexDesc) {
    return new UnsupportedUserIndex(cqlIdentifierFromUserInput(name), indexDesc.options());
  }
}
