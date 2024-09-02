package io.stargate.sgv2.jsonapi.mock;

import static io.stargate.sgv2.jsonapi.mock.TestListUtil.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.service.shredding.tables.RowId;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.*;
import java.util.function.Supplier;

/**
 * Classes that implement that generate a list of {@link WriteableTableRowFixture} for a {@link
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
public abstract class WriteableTableRowFixtureSupplier
    implements Supplier<List<WriteableTableRowFixture>> {

  protected final CqlFixture cqlFixture;

  protected WriteableTableRowFixtureSupplier(CqlFixture cqlFixture) {
    this.cqlFixture = cqlFixture;
  }

  /**
   * Generates a list of {@link WriteableTableRowFixture} that test different combinations of the
   * table. See sub-classes for what combinations are generated.
   *
   * <p>NOTE: sub-classes should implement the {@link #getInternal(List, List, List)} method it is
   * easier.
   */
  @Override
  public List<WriteableTableRowFixture> get() {

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
   * @return list of the {@link WriteableTableRowFixture} that should be tested against the table
   */
  protected abstract List<WriteableTableRowFixture> getInternal(
      List<ColumnMetadata> allColumnsMetadata,
      List<ColumnMetadata> keysMetadata,
      List<ColumnMetadata> nonKeyMetadata);

  /** Generate values for the given list of columns using the {@link CqlFixture} data generator. */
  protected Map<CqlIdentifier, Object> columnValues(List<ColumnMetadata> columns) {
    // Collectors.toMap does not handle a null value in a map.

    Map<CqlIdentifier, Object> values = new HashMap<>();
    columns.forEach(
        column -> values.put(column.getName(), cqlFixture.data().fromJSON(column.getType())));
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
  protected WriteableTableRow row(
      List<ColumnMetadata> setKeys, List<ColumnMetadata> setNonKeyColumns) {

    var allSetColumns = join(setKeys, setNonKeyColumns);
    var columnValues = columnValues(allSetColumns);

    List<Object> keyValues =
        setKeys.stream().map(columnMetadata -> columnValues.get(columnMetadata.getName())).toList();
    return new WriteableTableRow(new RowId(keyValues.toArray()), columnValues);
  }

  /**
   * Helper to generate a {@link WriteableTableRowFixture}, values are set using the {@link
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
  protected WriteableTableRowFixture fixture(
      List<ColumnMetadata> setKeysMetadata,
      List<ColumnMetadata> setNonKeyMetadata,
      List<ColumnMetadata> missingKeysMetadata,
      List<ColumnMetadata> missingNonKeyMetadata) {

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

    return new WriteableTableRowFixture(
        getClass(),
        cqlFixture,
        row(setKeysMetadata, setNonKeyMetadata),
        columnNames(setKeysMetadata),
        columnNames(setNonKeyMetadata),
        columnNames(missingKeysMetadata),
        columnNames(missingNonKeyMetadata),
        columnNames(unknownAllColumns),
        columnNames(unsupportedAllColumns));
  }

  /**
   * Generates one fixture for the table that includes all columns
   *
   * <p>Supported data types are checked, and added to the unsupported list
   */
  public static class AllColumns extends WriteableTableRowFixtureSupplier {

    public AllColumns(CqlFixture cqlFixture) {
      super(cqlFixture);
    }

    @Override
    protected List<WriteableTableRowFixture> getInternal(
        List<ColumnMetadata> allColumnsMetadata,
        List<ColumnMetadata> keysMetadata,
        List<ColumnMetadata> nonKeyMetadata) {
      List<WriteableTableRowFixture> fixtures = new ArrayList<>();

      var setKeysMetadata = keysMetadata;
      var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

      var setNonKeyMetadata = nonKeyMetadata;
      var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

      fixtures.add(
          fixture(setKeysMetadata, setNonKeyMetadata, missingKeysMetadata, missingNonKeyMetadata));
      return fixtures;
    }
  }

  /** Generates one fixture for each missing primary key */
  public static class MissingPrimaryKeys extends WriteableTableRowFixtureSupplier {

    public MissingPrimaryKeys(CqlFixture cqlFixture) {
      super(cqlFixture);
    }

    @Override
    protected List<WriteableTableRowFixture> getInternal(
        List<ColumnMetadata> allColumnsMetadata,
        List<ColumnMetadata> keysMetadata,
        List<ColumnMetadata> nonKeyMetadata) {
      List<WriteableTableRowFixture> fixtures = new ArrayList<>();

      var setNonKeyMetadata = nonKeyMetadata;
      var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

      testCombinations(keysMetadata, true, false)
          .forEach(
              combination -> {
                var setKeysMetadata = combination;
                var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

                fixtures.add(
                    fixture(
                        setKeysMetadata,
                        setNonKeyMetadata,
                        missingKeysMetadata,
                        missingNonKeyMetadata));
              });
      return fixtures;
    }
  }

  /** Generates one fixture for each combination of the non-primary key columns */
  public static class MissingNonKeyColumns extends WriteableTableRowFixtureSupplier {

    public MissingNonKeyColumns(CqlFixture cqlFixture) {
      super(cqlFixture);
    }

    @Override
    protected List<WriteableTableRowFixture> getInternal(
        List<ColumnMetadata> allColumnsMetadata,
        List<ColumnMetadata> keysMetadata,
        List<ColumnMetadata> nonKeyMetadata) {

      List<WriteableTableRowFixture> fixtures = new ArrayList<>();

      // We set all the primary keys
      var setKeysMetadata = keysMetadata;
      var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

      testCombinations(nonKeyMetadata, true, false)
          .forEach(
              combination -> {
                var setNonKeyMetadata = combination;
                var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

                fixtures.add(
                    fixture(
                        setKeysMetadata,
                        setNonKeyMetadata,
                        missingKeysMetadata,
                        missingNonKeyMetadata));
              });
      return fixtures;
    }
  }

  /**
   * Generates one fixture for each non-primary key column and gets the name wrong in the row, only
   * do it for the non-pk because errors for missing pk are different
   *
   * <p>The name of the column is changed using {@link CqlIdentifiers#mask(CqlIdentifier)}
   */
  public static class UnknownColumns extends WriteableTableRowFixtureSupplier {

    public UnknownColumns(CqlFixture cqlFixture) {
      super(cqlFixture);
    }

    @Override
    protected List<WriteableTableRowFixture> getInternal(
        List<ColumnMetadata> allColumnsMetadata,
        List<ColumnMetadata> keysMetadata,
        List<ColumnMetadata> nonKeyMetadata) {

      List<WriteableTableRowFixture> fixtures = new ArrayList<>();

      // We set all the primary keys
      var setKeysMetadata = keysMetadata;
      var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);

      // a row will all non key columns unknown
      var allUnknownColumns =
          nonKeyMetadata.stream()
              .map(
                  columnMetadata -> {
                    var maskedIdentifier = cqlFixture.identifiers().mask(columnMetadata.getName());
                    return TableMetadataBuilder.renameColumn(columnMetadata, maskedIdentifier);
                  })
              .toList();
      fixtures.add(
          fixture(
              setKeysMetadata,
              allUnknownColumns,
              missingKeysMetadata,
              difference(nonKeyMetadata, allUnknownColumns)));

      //  row for each non key column in the table, with one column name changed
      testReplicated(nonKeyMetadata)
          .forEach(
              entry -> {
                var setNonKeyMetadata = entry.getValue();
                // change the name of the column we are up to, so we have an unknown column
                var originalMetadata = setNonKeyMetadata.get(entry.getKey());
                var maskedIdentifier = cqlFixture.identifiers().mask(originalMetadata.getName());
                var maskedMetadata =
                    TableMetadataBuilder.renameColumn(originalMetadata, maskedIdentifier);
                setNonKeyMetadata.set(entry.getKey(), maskedMetadata);
                var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);

                fixtures.add(
                    fixture(
                        setKeysMetadata,
                        setNonKeyMetadata,
                        missingKeysMetadata,
                        missingNonKeyMetadata));
              });
      return fixtures;
    }
  }
}
