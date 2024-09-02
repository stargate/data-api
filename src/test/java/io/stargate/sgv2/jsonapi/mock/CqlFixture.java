package io.stargate.sgv2.jsonapi.mock;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.ArrayList;
import java.util.List;

/**
 * Once combination of CQL configuration to test against.
 *
 * <p>Call {@link #allFixtures()} to get all the possible combinations of CQL configurations of the
 * {@link CqlIdentifiers}, {@link CqlData} and {@link TableMetadataFixture} that are available.
 *
 * <p>So every table fixture is run with every combination of identifiers and data.Add a new table,
 * and it will be tested in all combinations.
 *
 * <p>Note using record because we want private instance state
 */
public class CqlFixture {

  /**
   * Returns all the possible combinations of CQL configurations of the {@link CqlIdentifiers},
   * {@link CqlData} and {@link TableMetadataFixture} that are available.
   */
  public static List<CqlFixture> allFixtures() {
    List<CqlFixture> fixtures = new ArrayList<>();

    for (CqlIdentifiers identifiers : CqlIdentifiersSource.ALL_CLASSES) {
      for (CqlData cqlData : CqlDataSource.ALL_SOURCES) {
        for (TableMetadataFixture tableFixture : TableMetadataFixtureSource.ALL_SOURCES) {
          fixtures.add(new CqlFixture(identifiers, cqlData, tableFixture));
        }
      }
    }
    return fixtures;
  }

  private final CqlIdentifiers identifiers;
  private final CqlData cqlData;
  private final TableMetadataFixture tableFixture;
  private final TableMetadata tableMetadata;

  public CqlFixture(
      CqlIdentifiers identifiers, CqlData cqlData, TableMetadataFixture tableFixture) {
    this.identifiers = identifiers;
    this.cqlData = cqlData;
    this.tableFixture = tableFixture;
    this.tableMetadata = tableFixture.tableMetadata(identifiers);
  }

  public CqlIdentifiers identifiers() {
    return identifiers;
  }

  public CqlData data() {
    return cqlData;
  }

  public TableMetadataFixture table() {
    return tableFixture;
  }

  public TableMetadata tableMetadata() {
    return tableMetadata;
  }

  @Override
  public String toString() {
    return String.format(
        "CqlFixture{identifiers=%s, cqlData=%s, tableFixture=%s}",
        identifiers, cqlData, tableFixture);
  }
}
