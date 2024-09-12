package io.stargate.sgv2.jsonapi.fixtures.containers.json;

import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.difference;
import static io.stargate.sgv2.jsonapi.fixtures.TestListUtil.testReplicated;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import io.stargate.sgv2.jsonapi.fixtures.identifiers.FixtureIdentifiers;
import io.stargate.sgv2.jsonapi.fixtures.tables.TableMetadataBuilder;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates one fixture for each non-primary key column and gets the name wrong in the row, only do
 * it for the non-pk because errors for missing pk are different
 *
 * <p>The name of the column is changed using {@link FixtureIdentifiers#mask(CqlIdentifier)}
 */
public class UnknownColumns extends JsonContainerFixtureBuilder {

  public UnknownColumns(CqlFixture cqlFixture) {
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
            difference(nonKeyMetadata, allUnknownColumns),
            List.of()));

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
                      missingNonKeyMetadata,
                      List.of()));
            });
    return fixtures;
  }
}
