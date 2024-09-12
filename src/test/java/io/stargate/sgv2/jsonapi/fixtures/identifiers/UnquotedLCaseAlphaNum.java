package io.stargate.sgv2.jsonapi.fixtures.identifiers;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class UnquotedLCaseAlphaNum extends BaseFixtureIdentifiers {

  @Override
  protected CqlIdentifier generateIdentifier(IdentifierUse use, int index) {
    return switch (use) {
      case KEY -> CqlIdentifier.fromCql("key" + index);
      case COLUMN -> CqlIdentifier.fromCql("col" + index);
      case TABLE -> CqlIdentifier.fromCql("table" + index);
      case KEYSPACE -> CqlIdentifier.fromCql("keyspace" + index);
    };
  }

  @Override
  public CqlIdentifier mask(String unquotedOriginal) {
    return CqlIdentifier.fromCql("masked" + unquotedOriginal);
  }
}
