package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.fixtures.TestListUtil;
import io.stargate.sgv2.jsonapi.service.shredding.JsonNamedValueContainer;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import io.stargate.sgv2.jsonapi.util.PrettyToStringBuilder;
import java.util.Arrays;
import java.util.List;

/**
 * A fixture for a {@link WriteableTableRow} that is used in tests, created by {@link
 * JsonContainerFixtureBuilder}
 *
 * <p>
 */
public record JsonContainerFixture(
    // Class that created the fixture, used in nice msg for test output
    Class<?> supplier,
    // The CQL fixture that the row is based on
    CqlFixture cqlFixture,
    // The row that to be tested
    JsonNamedValueContainer container,
    // Primary key columns that are set in the row
    List<ColumnMetadata> setKeys,
    // Non-primary key columns that have values set in the row
    List<ColumnMetadata> setNonKeyColumns,
    // Primary key columns that are missing from the row
    List<ColumnMetadata> missingKeys,
    // Non-primary keys columns that are missing from the row
    List<ColumnMetadata> missingNonKeyColumns,
    // Any columns in the row that are not in the table (PK and non PK)
    List<ColumnMetadata> unknownAllColumns,
    // Any columns in the row that are unsupported types (PK and non PK)
    List<ColumnMetadata> unsupportedAllColumns,
    // Any columns in the row that are out of range (PK and non PK)
    List<ColumnMetadata> outOfRangeAllColumns) {

  /** All the primary key and non-primary key columns that are set in the row */
  public List<ColumnMetadata> allSetColumns() {
    return TestListUtil.join(setKeys, setNonKeyColumns);
  }

  /** All the primary key and non-primary key columns that are missing in the row */
  public List<ColumnMetadata> allMissingColumns() {
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
    return toString(false);
  }

  public String toString(boolean pretty) {

    // avoid needing to keep this up to date with the fields on the record
    var sb = new PrettyToStringBuilder(supplier, pretty);

    sb.append("table", cqlFixture.table().toString());
    sb.append("identifiers", cqlFixture.identifiers().toString());
    sb.append("data", cqlFixture.data().toString());

    Arrays.stream(getClass().getDeclaredFields())
        .forEach(
            field -> {
              try {
                if (!List.class.isAssignableFrom(field.getType())) {
                  return;
                }

                List<ColumnMetadata> list = (List<ColumnMetadata>) field.get(this);
                if (list.isEmpty()) {
                  return;
                }
                sb.append(field.getName(), toString(list));
              } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
              }
            });
    return sb.toString();
  }

  private static String toString(List<ColumnMetadata> columns) {
    return String.join(
        ",",
        columns.stream()
            .map(
                metadata ->
                    String.format(
                        "%s(%s)",
                        metadata.getName().asCql(true), metadata.getType().asCql(false, true)))
            .toList());
  }
}
