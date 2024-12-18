package io.stargate.sgv2.jsonapi.fixtures;

import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.fixtures.data.DefaultData;
import io.stargate.sgv2.jsonapi.fixtures.data.FixtureData;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.BaseFixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.tables.TableFixture;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import java.util.ArrayList;
import java.util.List;

/**
 * Once combination of CQL configuration to test against.
 *
 * <p>Call {@link #allFixtures()} to get all the possible combinations of CQL configurations of the
 * {@link FixtureIdentifiers}, {@link FixtureData} and {@link TableFixture} that are available. Use
 * the overloads if you want to control the combinations, such as testing unsupported data types.
 *
 * <p>So every table fixture is run with every combination of identifiers and data.Add a new table,
 * and it will be tested in all combinations.
 *
 * <p>Note using record because we want private instance state
 */
public class CqlFixture {

  /**
   * Returns all the possible combinations of CQL configurations of the {@link FixtureIdentifiers},
   * {@link FixtureData} and {@link TableFixture} that are available.
   */
  public static List<CqlFixture> allFixtures() {
    return allFixtures(
        BaseFixtureIdentifiers.ALL_CLASSES, DefaultData.SUPPORTED, TableFixture.SUPPORTED);
  }

  /** See {@link #allFixtures()} */
  public static List<CqlFixture> allFixtures(
      List<FixtureIdentifiers> allIdentifiers,
      List<FixtureData> allCqlData,
      List<TableFixture> allTableFixture) {
    List<CqlFixture> fixtures = new ArrayList<>();

    for (FixtureIdentifiers identifiers : allIdentifiers) {
      for (FixtureData cqlData : allCqlData) {
        for (TableFixture tableFixture : allTableFixture) {
          fixtures.add(new CqlFixture(identifiers, cqlData, tableFixture));
        }
      }
    }
    return fixtures;
  }

  private final FixtureIdentifiers identifiers;
  private final FixtureData cqlData;
  private final TableFixture tableFixture;
  private final TableMetadata tableMetadata;
  private final TableSchemaObject tableSchemaObject;

  public CqlFixture(
      FixtureIdentifiers identifiers, FixtureData cqlData, TableFixture tableFixture) {
    this.identifiers = identifiers;
    this.cqlData = cqlData;
    this.tableFixture = tableFixture;
    this.tableMetadata = tableFixture.tableMetadata(identifiers);
    this.tableSchemaObject = TableSchemaObject.from(tableMetadata, new ObjectMapper());
  }

  public FixtureIdentifiers identifiers() {
    return identifiers;
  }

  public FixtureData data() {
    return cqlData;
  }

  public TableFixture table() {
    return tableFixture;
  }

  public TableMetadata tableMetadata() {
    return tableMetadata;
  }

  public TableSchemaObject tableSchemaObject() {
    return tableSchemaObject;
  }

  @Override
  public String toString() {
    return String.format(
        "CqlFixture{identifiers=%s, cqlData=%s, tableFixture=%s}",
        identifiers, cqlData, tableFixture);
  }
}
