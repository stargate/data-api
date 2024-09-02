package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.ArrayList;
import java.util.Arrays;
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
    List<CqlIdentifier> unknownAllColumns,
    // Any columns in the row that are unsupported types (non PK)
    List<CqlIdentifier> unsupportedAllColumns) {

  /** All the primary key and non-primary key columns that are set in the row */
  public List<CqlIdentifier> allSetColumns() {
    return TestListUtil.join(setKeys, setNonKeyColumns);
  }

  /** All the primary key and non-primary key columns that are missing in the row */
  public List<CqlIdentifier> allMissingColumns() {
    return TestListUtil.join(missingKeys, missingNonKeyColumns);
  }

  /**
   * Override because this is what is in the test output.
   *
   * <p>NOTE: only outputs for the lists that are not empty, otherwise this was getting too long.
   *
   * @return
   */
  @Override
  public String toString() {

    // avoid needing to keep this up to date with the fields on the record

    var fmtArgs = new ArrayList<Object>();
    var fmtString = new StringBuilder("%s{table=%s, identifiers=%s, data=%s");
    fmtArgs.add(supplier.getSimpleName());
    fmtArgs.add(cqlFixture.table());
    fmtArgs.add(cqlFixture.identifiers());
    fmtArgs.add(cqlFixture.data());

    Arrays.stream(getClass().getDeclaredFields())
        .forEach(
            field -> {
              try {
                if (!List.class.isAssignableFrom(field.getType())) {
                  return;
                }

                List<?> list = (List<?>) field.get(this);
                if (list.isEmpty()) {
                  return;
                }
                fmtString.append(", %s=%s");
                fmtArgs.add(field.getName());
                fmtArgs.add(list);
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
    fmtString.append("}");
    return String.format(fmtString.toString(), fmtArgs.toArray());
  }
}
