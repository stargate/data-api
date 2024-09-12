package io.stargate.sgv2.jsonapi.fixtures.identifiers;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class QuotedMixedCaseAllChar extends BaseFixtureIdentifiers {

  @Override
  protected CqlIdentifier generateIdentifier(IdentifierUse use, int index) {
    return switch (use) {
      case KEY -> CqlIdentifier.fromCql(quote("Key - " + index));
      case COLUMN -> CqlIdentifier.fromCql(quote("Col - " + index));
      case TABLE -> CqlIdentifier.fromCql(quote("Table - " + index));
      case KEYSPACE -> CqlIdentifier.fromCql(quote("Keyspace - " + index));
    };
  }

  @Override
  public CqlIdentifier mask(String unquotedOriginal) {
    return CqlIdentifier.fromCql(quote("Masked - " + unquotedOriginal));
  }
}
