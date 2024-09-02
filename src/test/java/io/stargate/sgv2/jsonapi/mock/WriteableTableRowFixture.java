package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.List;

/**
 * A fixture for a {@link WriteableTableRow} that is used in tests, created by {@link
 * WriteableTableRowFixtureSupplier}
 *
 * <p>
 */
public record WriteableTableRowFixture(
    // Class that created the fixture, used in nice msg for test output
    Class<?> supplier,
    // The CQL fixture that the row is based on
    CqlFixture cqlFixture,
    // The row that to be tested
    WriteableTableRow row,
    // Primary key columns that are set in the row
    List<CqlIdentifier> setKeys,
    // Non-primary key columns that have values set in the row
    List<CqlIdentifier> setNonKeyColumns,
    // Primary key columns that are missing from the row
    List<CqlIdentifier> missingKeys,
    // Non-primary keys columns that are missing from the row
    List<CqlIdentifier> missingNonKeyColumns,
    // Any columns in the row that are not in the table (PK and non PK)
    List<CqlIdentifier> unknownAllColumns) {

  /** All the primary key and non-primary key columns that are set in the row */
  public List<CqlIdentifier> allSetColumns() {
    return TestListUtil.join(setKeys, setNonKeyColumns);
  }

  /** All the primary key and non-primary key columns that are missing in the row */
  public List<CqlIdentifier> allMissingColumns() {
    return TestListUtil.join(missingKeys, missingNonKeyColumns);
  }

  @Override
  public String toString() {
    // This is used in the test output, reducing the verbosity
    // do not want to print the row, it is too verbose
    return String.format(
        "%s{table=%s, identifiers=%s, data=%s, setKeys=%s, missingKeys=%s, setNonKeyColumns=%s, missingNonKeyColumns=%s, unknownAllColumns=%s}",
        supplier.getSimpleName(),
        cqlFixture.table(),
        cqlFixture.identifiers(),
        cqlFixture.data(),
        setKeys,
        missingKeys,
        setNonKeyColumns,
        missingNonKeyColumns,
        unknownAllColumns);
  }
}
