package io.stargate.sgv2.jsonapi.service.operation.keyspaces;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link DropKeyspaceOperation} produces CQL via the driver's {@code SchemaBuilder}
 * rather than via {@code String.format} interpolation. The previous {@code "DROP KEYSPACE IF EXISTS
 * \"%s\""} interpolation could be broken by a name containing a double quote; {@code
 * CqlIdentifier.fromInternal} escapes those by doubling, so the resulting CQL stays well-formed.
 */
class DropKeyspaceOperationTest {

  @Test
  public void buildsExpectedCqlForSimpleName() {
    var op = new DropKeyspaceOperation("red_star_belgrade");
    String cql = op.buildStatement().getQuery();
    assertThat(cql).contains("DROP KEYSPACE IF EXISTS").contains("red_star_belgrade");
  }

  @Test
  public void escapesEmbeddedDoubleQuoteInIdentifier() {
    var op = new DropKeyspaceOperation("foo\"bar");
    String cql = op.buildStatement().getQuery();
    // Doubled quote keeps the identifier well-formed; the prior String.format sink would have
    // produced "foo"bar" — a broken identifier that could form an injection vector.
    assertThat(cql).contains("\"foo\"\"bar\"").doesNotContain("\"foo\"bar\"");
  }
}
