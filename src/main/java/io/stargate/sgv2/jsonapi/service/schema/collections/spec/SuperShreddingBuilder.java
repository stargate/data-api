package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.Describable;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDef;
import io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingMetadata.IndexDefs;
import java.util.*;
import java.util.stream.Stream;

/**
 * General pattern for defining the properties of a super-shredding "table" and then building
 * objects from that.
 *
 * <p>Building these objects is tied up with how we create the statements to build a table, how we
 * build a predicate to test for a table, and how we build test data. Without repeating the table
 * cql too many times and creating fragile tests that depend on cql strings. See the test class
 * <code>SuperShreddingBuilderTest</code>
 *
 * <p>From the logical representation on this builder, we can create:
 *
 * <ul>
 *   <li><code>cql</code> for testing (below) via {@link #cql()}
 *   <li>{@link TableMetadata} and {@link
 *       com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} for testing (below), via
 *       {@link #metadata()}
 *   <li>{@link com.datastax.oss.driver.api.core.cql.SimpleStatement} for creating a table at run
 *       time via TODO
 *   <li>{@link SuperShreddingTablePredicate} for runtime testing if TableMetadata represents a
 *       super shredding table via {@link #predicate()}
 * </ul>
 *
 * <p>The builder creates a list of {@link SuperShreddingComponent} which can be either a Table or
 * the Index (s) needed. The different builders use different types for these components.
 *
 * @param <T> Type of the object that represents the Super Shredding Component, such as string for
 *     cql
 * @param <U> Type of the builder itself, so that we can return a reference to this builder.
 */
public abstract class SuperShreddingBuilder<T, U extends SuperShreddingBuilder<T, U>> {

  // The comment for a table it a member of the table "options" and must have a
  // CqlIdentifier for a name
  protected static final CqlIdentifier TABLE_OPTION_COMMENT_IDENTIFIER =
      CqlIdentifier.fromInternal("comment");

  protected final SuperShreddingBinding.Builder bindingBuilder = SuperShreddingBinding.builder();
  // created in build(), private to force use of binding() accessor to check null
  private SuperShreddingBinding binding;

  protected boolean ifNotExists = true;
  protected String comment;

  /** Geta a new {@link SuperShreddingCQLBuilder} that can be used to build a cql string. */
  public static SuperShreddingCQLBuilder cql() {
    return new SuperShreddingCQLBuilder();
  }

  /**
   * Get a new {@link SuperShreddingMetadataBuilder} that can be used to build {@link TableMetadata}
   * and {@link com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata} objects.
   */
  public static SuperShreddingMetadataBuilder metadata() {
    return new SuperShreddingMetadataBuilder();
  }

  /**
   * Get a new {@link SuperShreddingPredicateBuilder} that can be used to build a {@link
   * SuperShreddingTablePredicate}
   */
  public static SuperShreddingPredicateBuilder predicate() {
    return new SuperShreddingPredicateBuilder();
  }

  /** Implementors must override this method to return a reference to this builder. */
  protected abstract U self();

  /**
   * Implementations must implement and create all the components needed for the super shredding
   * table.
   */
  protected abstract List<SuperShreddingComponent<T>> buildInternal();

  protected SuperShreddingBinding binding() {
    Objects.requireNonNull(binding, "binding must be set by build()");
    return binding;
  }

  public U withIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
    return self();
  }

  public U withKeyspace(CqlIdentifier keyspace) {
    bindingBuilder.withKeyspace(keyspace);
    return self();
  }

  public U withCollection(CqlIdentifier collection) {
    bindingBuilder.withCollection(collection);
    return self();
  }

  public U withVector(int vectorLength, String similarityFunction, String sourceModel) {
    bindingBuilder.withVector(vectorLength, similarityFunction, sourceModel);
    return self();
  }

  public U withLexical(String indexAnalyzer) {
    bindingBuilder.withLexical(indexAnalyzer);
    return self();
  }

  public U withComment(String comment) {
    this.comment = comment;
    return self();
  }

  /**
   * Builds all the components for the table, and returns only the value of the (first) Table
   * component. Use this to quickly get just the (say) "create table" cql.
   */
  public T buildTableOnly() {
    return build().stream()
        .filter(c -> c.type() == SuperShreddingComponentType.TABLE)
        .map(SuperShreddingComponent::value)
        .findFirst()
        .orElse(null);
  }

  /**
   * Builds all the components for this super shredding table, the table and the indexes as defined
   * in the builder.
   *
   * <p>NOTE: to implementors, implement {@link #buildInternal()} so the superShreddingDef is set.
   *
   * @return List of {@link SuperShreddingComponent}s needed for the super shredding table.
   */
  public List<SuperShreddingComponent<T>> build() {
    binding = bindingBuilder.build();
    return buildInternal();
  }

  /** The type of component that is being built for the super shredding table */
  public enum SuperShreddingComponentType {
    TABLE,
    INDEX
  }

  /**
   * Holds a component of a super shredding table, such as the table or index. These are created by
   * the {@link SuperShreddingBuilder} implementations.
   *
   * @param identifier the name, table name or index name.
   * @param type the type of component, either table or index
   * @param value the value of the component, such as the table definition or index definition, or
   *     string
   * @param <T> The type of the value of the component, e,g, String or TableMetadata
   */
  public record SuperShreddingComponent<T>(
      CqlIdentifier identifier, SuperShreddingComponentType type, T value) {

    /** Does its best to get CQL from whatever type of value we have. For testing. */
    @VisibleForTesting
    String asCql() {
      var cql =
          switch (value) {
            case Describable d -> d.describe(false).trim();
            case String s -> s.trim();
            default ->
                throw new IllegalArgumentException("Unsupported value type: " + value.getClass());
          };
      // there is a small bug in the river IndexMetadata where it does not append ";" for a
      // CUSTOM INDEX, just check so they are all the same.
      return cql.endsWith(";") ? cql : cql + ";";
    }
  }

  /**
   * Gets the index definitions and options for the super shredding table based on {@link
   * SuperShreddingBinding}
   *
   * <p>This pulls the options from the {@link SuperShreddingBinding} and puts them into maps of the
   * values each index definition needs
   */
  protected Stream<IndexDef> indexDefs(SuperShreddingBinding binding) {

    Stream.Builder<IndexDef> builder = Stream.builder();

    IndexDefs.REQUIRED.forEach(builder);
    if (this.binding.isVectorDefined()) {
      builder.add(IndexDefs.QUERY_VECTOR_VALUE);
    }

    if (this.binding.isLexicalDefined()) {
      builder.add(IndexDefs.QUERY_LEXICAL_VALUE);
    }
    return builder.build();
  }
}
