package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.*;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.fixtures.types.CqlTypesForTesting;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValue;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.UnorderedJsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonPath;

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
public abstract class JsonContainerFixtureBuilder
    implements Supplier<List<JsonContainerFixture>> {

  protected final CqlFixture cqlFixture;

  protected JsonContainerFixtureBuilder(CqlFixture cqlFixture) {
    this.cqlFixture = cqlFixture;
  }

  /**
   * Generates a list of {@link JsonContainerFixture} that test different combinations of the
   * table. See sub-classes for what combinations are generated.
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
  protected Map<ColumnMetadata, JsonNamedValue> columnValues(List<ColumnMetadata> columns) {
    // Collectors.toMap does not handle a null value in a map.

    Map<ColumnMetadata, JsonNamedValue> values = new HashMap<>();
    columns.forEach(
        metadata -> {
          var jsonNamedValue = new JsonNamedValue(
              // get the asInternal - we do not want any quotes, this is the value that would be pulled from JSON doc
              JsonPath.rootBuilder().property(metadata.getName().asInternal()).build(),
              cqlFixture.data().fromJSON(metadata.getType()));
          values.put(metadata, jsonNamedValue);
        });
    return values;
  }

  /**
   * Helper to create a WriteableTableRow instance,values are set using the {@link CqlFixture} data
   * generator.
   *
   * @param setKeys keys that should have a value set
   * @param setNonKeyColumns non-key columns that should have a value set
   * @return confifgured WriteableTableRow
   */
  protected JsonNamedValueContainer jsonContainer(
      List<ColumnMetadata> setKeys, List<ColumnMetadata> setNonKeyColumns) {

    var allSetColumns = join(setKeys, setNonKeyColumns);
    var columnValues = columnValues(allSetColumns);

    return new UnorderedJsonNamedValueContainer(columnValues.values());
  }

  /**
   * Helper to generate a {@link JsonContainerFixture}, values are set using the {@link
   * CqlFixture} data generator.
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
