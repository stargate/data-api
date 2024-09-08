package io.stargate.sgv2.jsonapi.fixtures.identifiers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.fixtures.tables.TableFixture;

import java.util.List;

/**
 * Interface for a class that returns {@link CqlIdentifier} that can be used when create a {@link
 * TableFixture}
 *
 * <p>Interface allows for testing different combinations of identifiers, e.g. quoted, unquoted,
 * spaces etc. Use the <code>get*</code> function to get an identifier that will be same the same
 * for the same index. Use <code>random*</code> to get an identifier that will be different from
 * each call.
 *
 * <p>Implementations should include something in the identifier to make it clear if it is a column
 * or key etc.
 */
public interface FixtureIdentifiers {

  List<FixtureIdentifiers> ALL_CLASSES = List.of(
      new UnquotedLCaseAlphaNum(),
      new UnquotedMixedCaseAlphaNum(),
      new QuotedMixedCaseAlphaNum(),
      new QuotedMixedCaseAllChar());

  CqlIdentifier getKey(int index);

  CqlIdentifier randomKey();

  CqlIdentifier getColumn(int index);

  CqlIdentifier randomColumn();

  CqlIdentifier getTable(int index);

  CqlIdentifier randomTable();

  CqlIdentifier getKeyspace(int index);

  CqlIdentifier randomKeyspace();

  /**
   * Return an identifier that is prefixed with in a way that follows the CQL rules for this type of
   * identifier. Used when we want to test using a column name that may be wrong.
   *
   * @param identifier
   * @return
   */
  CqlIdentifier mask(CqlIdentifier identifier);
}
