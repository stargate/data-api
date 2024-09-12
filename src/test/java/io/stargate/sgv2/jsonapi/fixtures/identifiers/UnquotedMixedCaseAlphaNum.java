package io.stargate.sgv2.jsonapi.fixtures.identifiers;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class UnquotedMixedCaseAlphaNum extends BaseFixtureIdentifiers {

  @Override
  protected CqlIdentifier generateIdentifier(IdentifierUse use, int index) {
    return switch (use) {
      case KEY -> CqlIdentifier.fromCql("Key" + index);
      case COLUMN -> CqlIdentifier.fromCql("Col" + index);
      case TABLE -> CqlIdentifier.fromCql("Table" + index);
      case KEYSPACE -> CqlIdentifier.fromCql("Keyspace" + index);
    };
  }

  @Override
  public CqlIdentifier mask(String unquotedOriginal) {
    return CqlIdentifier.fromCql("Masked" + unquotedOriginal);
  }
}
