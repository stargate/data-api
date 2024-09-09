package io.stargate.sgv2.jsonapi.fixtures.identifiers;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.util.Strings;
import io.stargate.sgv2.jsonapi.fixtures.CqlFixture;
import java.util.Random;

/**
 * Implementstions of the {@link FixtureIdentifiers} interface that can be used when create a {@link
 * CqlFixture}.
 *
 * <p>See docs for {@link CqlIdentifier} , goal is to support the different types of identifiers
 * listed in the docs there.
 *
 * <p>Implementations generate Identifiers with different characteristics, e.g. quoted, unquoted,
 * spaces etc.
 *
 * <p>Add implementations of this class as inner classes and add them to the {@link #ALL_CLASSES}
 * list so they are picked up by {@link CqlFixture}
 *
 * <p>The name of the subclass is used in the test description.
 */
public abstract class BaseFixtureIdentifiers implements FixtureIdentifiers {

  private final Random random = new Random();

  protected enum IdentifierUse {
    KEY,
    COLUMN,
    TABLE,
    KEYSPACE;
  }

  protected static String quote(String identifier) {
    return "\"" + identifier + "\"";
  }

  /**
   * Sublcasses should implement to generate the identifier based on the use and index.
   *
   * @param use the use of the identifier, e.g. key, column, table, keyspace
   * @param index the index of the identifier, should be included in the name of the identifier
   * @return the generated identifier
   */
  protected abstract CqlIdentifier generateIdentifier(IdentifierUse use, int index);

  protected abstract CqlIdentifier mask(String unquotedOriginal);

  /** Get a positive int so the toString() only has numbers, negative will have "-" */
  private int nextPositiveInt() {
    return random.nextInt(Integer.MAX_VALUE);
  }

  @Override
  public CqlIdentifier getKey(int index) {
    return generateIdentifier(IdentifierUse.KEY, index);
  }

  @Override
  public CqlIdentifier randomKey() {
    return generateIdentifier(IdentifierUse.KEY, nextPositiveInt());
  }

  @Override
  public CqlIdentifier getColumn(int index) {
    return generateIdentifier(IdentifierUse.COLUMN, index);
  }

  @Override
  public CqlIdentifier randomColumn() {
    return generateIdentifier(IdentifierUse.COLUMN, nextPositiveInt());
  }

  @Override
  public CqlIdentifier getTable(int index) {
    return generateIdentifier(IdentifierUse.TABLE, index);
  }

  @Override
  public CqlIdentifier randomTable() {
    return generateIdentifier(IdentifierUse.TABLE, nextPositiveInt());
  }

  @Override
  public CqlIdentifier getKeyspace(int index) {
    return generateIdentifier(IdentifierUse.KEYSPACE, index);
  }

  @Override
  public CqlIdentifier randomKeyspace() {
    return generateIdentifier(IdentifierUse.KEYSPACE, nextPositiveInt());
  }

  public CqlIdentifier mask(CqlIdentifier identifier) {
    // neem to remove the double quotes so the source can mask and then quote the whole thing
    return mask(Strings.unDoubleQuote(identifier.asCql(true)));
  }

  /** Override so in the tests the toString() only has the class name of the identifier class. */
  @Override
  public String toString() {
    return getClass().getSimpleName();
  }
}
