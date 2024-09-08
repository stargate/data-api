package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;

import java.util.ArrayList;
import java.util.List;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.difference;
import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.testReplicated;

/**
 * Generate a fixture for combinations of the non primary key columns and adds the sets columns to {@link JsonContainerFixture#outOfRangeAllColumns()}
 */
public class EachNonKeyOutOfRange extends JsonContainerFixtureBuilder {

  public EachNonKeyOutOfRange(CqlFixture cqlFixture) {
    super(cqlFixture);
  }

  @Override
  protected List<JsonContainerFixture> getInternal(
      List<ColumnMetadata> allColumnsMetadata,
      List<ColumnMetadata> keysMetadata,
      List<ColumnMetadata> nonKeyMetadata) {

    List<JsonContainerFixture> fixtures = new ArrayList<>();

    // We set all the primary keys
    var setKeysMetadata = keysMetadata;
    var setNonKeyMetadata = nonKeyMetadata;
    var missingKeysMetadata = difference(keysMetadata, setKeysMetadata);
    var missingNonKeyMetadata = difference(nonKeyMetadata, setNonKeyMetadata);
    var outOfRangeAllColumns = nonKeyMetadata;

    // do not add a row that has all columns set, only do one at a time because initinity types need to be tested.

    //  row for each non key column in the table, with one column set
    testReplicated(nonKeyMetadata)
        .forEach(
            entry -> {
              var innerSetNonKeyMetadata = List.of(entry.getValue().get(entry.getKey()));
              var innerMissingNonKeyMetadata = difference(nonKeyMetadata, innerSetNonKeyMetadata);
              var innerOutOfRangeAllColumns = innerSetNonKeyMetadata;

              fixtures.add(
                  fixture(
                      setKeysMetadata,
                      innerSetNonKeyMetadata,
                      missingKeysMetadata,
                      innerMissingNonKeyMetadata,
                      innerOutOfRangeAllColumns));
            });
    return fixtures;
  }
}