package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import java.util.List;

/**
 * A {@link SuperShreddingBuilder} to create the {@link SuperShreddingTablePredicate}.
 *
 * <p>For now only creates a {@link SuperShreddingComponentType#TABLE} component, future work to
 * create index components. See {@link SuperShreddingBuilder} for more details.
 */
public class SuperShreddingPredicateBuilder
    extends SuperShreddingBuilder<SuperShreddingTablePredicate, SuperShreddingPredicateBuilder> {

  private boolean strict = true;

  protected SuperShreddingPredicateBuilder() {}

  @Override
  protected SuperShreddingPredicateBuilder self() {
    return this;
  }

  public SuperShreddingPredicateBuilder withStrict(boolean strict) {
    this.strict = strict;
    return this;
  }

  @Override
  public List<SuperShreddingComponent<SuperShreddingTablePredicate>> buildInternal() {

    var predicate = new SuperShreddingTablePredicate(strict, superShreddingDef);
    return List.of(
        new SuperShreddingComponent<>(
            superShreddingDef.collection(), SuperShreddingComponentType.TABLE, predicate));
  }
}
