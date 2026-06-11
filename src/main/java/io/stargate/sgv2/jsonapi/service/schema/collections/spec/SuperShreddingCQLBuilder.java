package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingCQL.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToCQL;

import java.util.*;
import java.util.stream.Stream;
import org.apache.commons.text.StringSubstitutor;

/**
 * A {@link SuperShreddingBuilder} that builds dynamic CQL from the config provided to the builder.
 *
 * <p>NOTE: this class is *not* used in production, it is only used by testing. It exists in the
 * regular jar because it is easier to keep all the super shredding code in one place. See {@link
 * SuperShreddingBuilder} for the testing process.
 *
 * <p>Create via {@link SuperShreddingBuilder#cql()}
 */
public class SuperShreddingCQLBuilder
    extends SuperShreddingBuilder<String, SuperShreddingCQLBuilder> {

  private boolean collapseWhitespace = true;

  SuperShreddingCQLBuilder() {}

  @Override
  protected SuperShreddingCQLBuilder self() {
    return this;
  }

  public SuperShreddingCQLBuilder withCollapseWhitespace(boolean collapseWhitespace) {
    this.collapseWhitespace = collapseWhitespace;
    return this;
  }

  @Override
  public List<SuperShreddingComponent<String>> buildInternal() {

    List<SuperShreddingComponent<String>> components = new ArrayList<>();
    components.add(
        new SuperShreddingComponent<>(
            superShreddingDef.collection(), SuperShreddingComponentType.TABLE, tableCQL()));
    indexCQL().forEach(components::add);
    return components;
  }

  private String tableCQL() {

    // building out the vars for the CQL templates
    Map<String, String> vars = new HashMap<>();
    if (ifNotExists) {
      vars.put("IF_NOT_EXISTS", "IF NOT EXISTS");
    }
    vars.put("KEYSPACE", cqlIdentifierToCQL(superShreddingDef.keyspace()));
    vars.put("TABLE", cqlIdentifierToCQL(superShreddingDef.collection()));

    if (superShreddingDef.isVectorDefined()) {
      vars.put(
          "VECTOR_COLUMN",
          new StringSubstitutor(Map.of("VECTOR_DIM", superShreddingDef.vectorLength()))
              .replace(CQL.TABLE_VECTOR_COLUMN_TEMPLATE));
    }

    if (superShreddingDef.isLexicalDefined()) {
      vars.put("LEXICAL_COLUMN", CQL.TABLE_LEXICAL_COLUMN_TEMPLATE);
    }

    if (comment != null) {
      vars.put(
          "COMMENT_CLAUSE",
          new StringSubstitutor(Map.of("COMMENT", comment))
              .replace(CQL.TABLE_COMMENT_CLAUSE_TEMPLATE));
    }

    var result = new StringSubstitutor(vars).replace(CQL.CREATE_TABLE_TEMPLATE);
    return collapseWhitespace ? collapseWhitespace(result) : result;
  }

  private Stream<SuperShreddingComponent<String>> indexCQL() {

    // get all the indexes this super shredding table should have
    var defsAndOptions = indexDefsAndOptions(superShreddingDef);

    // For each of the IndexDef, we need to get the CQL to build it
    var cqlAndDefs =
        defsAndOptions.indexDefs().stream()
            .map(IndexCQLAndDefs.ALL_INDEXES_BY_INDEX_DEF::get)
            .filter(Objects::nonNull)
            .toList();

    // sanity check
    if (cqlAndDefs.size() != defsAndOptions.indexDefs().size()) {
      throw new IllegalStateException("cqlAndDefs.size() != defsAndOptions.indexDefs().size()");
    }

    // Start building up the sub vars we need for all the index cql templates.
    Map<String, String> allIndexVars = new HashMap<>();

    // For indexes, if the def of the cql index has a clause template (like the config for
    // a vector index) we need to get those from the defsAndOptions created from superShreddingDef
    // run the clause template, and add the clause to our index vars
    for (IndexCQLAndDef cqlAndDef : cqlAndDefs) {
      if (cqlAndDef.clauseTemplate() != null) {
        // run the template for this clause, blindly get options from defsAndOptions because
        // null and empty are OK, If we get a clause back, then put that into the index vars
        // e.g. look at LEXICAL_WITH_OPTIONS_TEMPLATE

        cqlAndDef
            .clauseTemplate()
            .format(defsAndOptions.indexOptions().get(cqlAndDef.indexDef()))
            .map(clause -> allIndexVars.put(cqlAndDef.clauseTemplate().toKeyName(), clause));
      }
    }

    if (ifNotExists) {
      allIndexVars.put("IF_NOT_EXISTS", "IF NOT EXISTS");
    }

    // using internal the keyspace and table names because the collection name is
    // used as part of the index name, so we dont want quotes on them
    // NOTE: INDEXES templates MUST put the quotes on
    allIndexVars.put("KEYSPACE", superShreddingDef.keyspace().asInternal());
    allIndexVars.put("TABLE", superShreddingDef.collection().asInternal());

    var substitutor = new StringSubstitutor(allIndexVars);
    return cqlAndDefs.stream()
        .map(
            cqlAndDef -> {
              var cql = substitutor.replace(cqlAndDef.cql());

              return new SuperShreddingComponent<>(
                  cqlAndDef.indexDef().indexName(superShreddingDef.collection()),
                  SuperShreddingComponentType.INDEX,
                  collapseWhitespace ? collapseWhitespace(cql) : cql);
            });
  }
}
