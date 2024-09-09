package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.difference;
import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.testCombinations;

import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import java.util.ArrayList;
import java.util.List;

/** Generates one fixture for each combination of the non-primary key columns */
public class MissingNonKeyColumns extends JsonContainerFixtureBuilder {

  public MissingNonKeyColumns(CqlFixture cqlFixture) {
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
                      missingNonKeyMetadata,
                      List.of()));
            });
    return fixtures;
  }
}
