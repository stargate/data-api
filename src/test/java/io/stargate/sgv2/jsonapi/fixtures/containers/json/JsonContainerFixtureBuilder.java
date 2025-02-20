package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.fixtures.data.AllNullValues;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNodeDecoder;
import io.stargate.sgv2.jsonapi.service.shredding.tables.JsonNamedValueFactory;
import java.util.*;
import java.util.function.Supplier;

/**
 * Classes that implement that generate a list of {@link JsonContainerFixture} for a {@link
 * CqlFixture}, returns a list because the supplier functions generate different combinations of
 * rows to be inserted into the table.
 *
 * <p>Create inner subclasses, there is no list of all suppliers it would not make sense. i.e. we
 * have a supplier that generates rows that are missing primary keys, and another that generates
 * row's with PK's and diff combinations of columns. See {@link MissingPrimaryKeys} and {@link
 * MissingNonKeyColumns}.
 *
 * <p>The name of the subclass is used in the test description.
 */
public abstract class JsonContainerFixtureBuilder implements Supplier<List<JsonContainerFixture>> {

  protected final CqlFixture cqlFixture;

  protected JsonContainerFixtureBuilder(CqlFixture cqlFixture) {
    this.cqlFixture = cqlFixture;
  }

  /**
   * Generates a list of {@link JsonContainerFixture} that test different combinations of the table.
   * See sub-classes for what combinations are generated.
   *
   * <p>NOTE: sub-classes should implement the {@link #getInternal(List, List, List)} method it is
   * easier.
   */
  @Override
  public List<JsonContainerFixture> get() {

    List<ColumnMetadata> keysMetadata = cqlFixture.tableMetadata().getPrimaryKey();
    // Table metadata returns all the columns, including primary keys, from getColumns()
    // we often need to know what are PK's and what are regular so splitting to make the other code
    // a little easier
    List<ColumnMetadata> allColumnsMetadata =
        cqlFixture.tableMetadata().getColumns().values().stream().toList();
    var nonKeyMetadata = difference(allColumnsMetadata, keysMetadata);

    return getInternal(allColumnsMetadata, keysMetadata, nonKeyMetadata);
  }

  /**
   * Helper for sub-classes, is called with the list of columns that makes it easier to generate a
   * row.
   *
   * @param allColumnsMetadata all columns in the table, primary keys and non keys
   * @param keysMetadata only the primary key columns
   * @param nonKeyMetadata only the non-primary key columns
   * @return list of the {@link JsonContainerFixture} that should be tested against the table
   */
  protected abstract List<JsonContainerFixture> getInternal(
      List<ColumnMetadata> allColumnsMetadata,
      List<ColumnMetadata> keysMetadata,
      List<ColumnMetadata> nonKeyMetadata);

  /** Generate values for the given list of columns using the {@link CqlFixture} data generator. */
  protected JsonNamedValueContainer columnValues(
      Set<ColumnMetadata> primaryKeyColumns, List<ColumnMetadata> columns) {
    // Collectors.toMap does not handle a null value in a map.

    var jsonDoc = new ObjectMapper().createObjectNode();

    columns.forEach(
        metadata -> {
          // For AllNullValues FixtureData, we don't want to set primaryKey columns value as null or
          // ignore,
          // That will cause exception DocumentException.Code.MISSING_PRIMARY_KEY_COLUMNS
          // So when this situation is tested, switch to DefaultData to get the non-null JsonLiteral
          if (cqlFixture.data() instanceof AllNullValues allNullValues
              && primaryKeyColumns.contains(metadata)) {
            jsonDoc.set(
                cqlIdentifierToJsonKey(metadata.getName()),
                new DefaultData().fromJSON(metadata.getType()));
          } else {
            jsonDoc.set(
                cqlIdentifierToJsonKey(metadata.getName()),
                cqlFixture.data().fromJSON(metadata.getType()));
          }
        });
    return new JsonNamedValueFactory(cqlFixture.tableSchemaObject(), JsonNodeDecoder.DEFAULT)
        .create(jsonDoc);
  }

  /**
   * Helper to create a {@link JsonNamedValueContainer} instance,values are set using the {@link
   * CqlFixture} data generator.
   *
   * @param setKeys keys that should have a value set
   * @param setNonKeyColumns non-key columns that should have a value set
   * @return configured {@link JsonNamedValueContainer} with named values based on the setKeys and
   *     setNonKeyColumns
   */
  protected JsonNamedValueContainer jsonContainer(
      List<ColumnMetadata> setKeys, List<ColumnMetadata> setNonKeyColumns) {

    var allSetColumns = join(setKeys, setNonKeyColumns);
    return columnValues(new HashSet<>(setKeys), allSetColumns);
  }

  /**
   * Helper to generate a {@link JsonContainerFixture}, values are set using the {@link CqlFixture}
   * data generator.
   *
   * <p>The unknownAllColumns and unsupportedAllColumns are generated based on the values that are
   * set.
   *
   * @param setKeysMetadata the primary key columns to set a value for
   * @param setNonKeyMetadata the non-primary key columns to set a value for
   * @param missingKeysMetadata the primary key columns that are missing from the row
   * @param missingNonKeyMetadata the non-primary key columns that are missing from the row
   * @return configured WriteableTableRowFixture
   */
  protected JsonContainerFixture fixture(
      List<ColumnMetadata> setKeysMetadata,
      List<ColumnMetadata> setNonKeyMetadata,
      List<ColumnMetadata> missingKeysMetadata,
      List<ColumnMetadata> missingNonKeyMetadata,
      List<ColumnMetadata> outOfRangeAllColumns) {

    var allSetMetadata = join(setKeysMetadata, setNonKeyMetadata);
    var unknownAllColumns =
        allSetMetadata.stream()
            .filter(
                columnMetadata ->
                    !cqlFixture.tableMetadata().getColumns().containsKey(columnMetadata.getName()))
            .toList();

    var unsupportedAllColumns =
        allSetMetadata.stream()
            .filter(columnMetadata -> !CqlTypesForTesting.isSupportedForInsert(columnMetadata))
            .toList();

    return new JsonContainerFixture(
        getClass(),
        cqlFixture,
        jsonContainer(setKeysMetadata, setNonKeyMetadata),
        setKeysMetadata,
        setNonKeyMetadata,
        missingKeysMetadata,
        missingNonKeyMetadata,
        unknownAllColumns,
        unsupportedAllColumns,
        outOfRangeAllColumns);
  }
}
