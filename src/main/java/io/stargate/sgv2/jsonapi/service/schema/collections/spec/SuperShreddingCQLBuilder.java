package io.stargate.sgv2.jsonapi.service.schema.collections.spec;

import org.apache.commons.text.StringSubstitutor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.stargate.sgv2.jsonapi.service.schema.collections.spec.SuperShreddingCQL.*;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToCQL;

public class SuperShreddingCQLBuilder extends SuperShreddingBuilder<String, SuperShreddingCQLBuilder> {

  private boolean collapseWhitespace = true;
  private String comment;

  @Override
  protected SuperShreddingCQLBuilder self() {
    return this;
  }

  public SuperShreddingCQLBuilder withComment(String comment) {
    this.comment = comment;
    return this;
  }

  public SuperShreddingCQLBuilder withCollapseWhitespace(boolean collapseWhitespace) {
    this.collapseWhitespace = collapseWhitespace;
    return this;
  }

  @Override
  public List<SuperShreddingComponent<String>> build() {

    List<SuperShreddingComponent<String>> components = new ArrayList<>();
    components.add(new SuperShreddingComponent<>(collection, SuperShreddingComponentType.TABLE, tableCQL()));
    indexCQL().forEach(components::add);
    return components;
  }

  private String tableCQL() {

    Map<String, String> vars = new HashMap<>();
    if (ifNotExists) {
      vars.put("IF_NOT_EXISTS", "IF NOT EXISTS");
    }
    vars.put("KEYSPACE", cqlIdentifierToCQL(keyspace));
    vars.put("TABLE", cqlIdentifierToCQL(collection));

    if (vectorLength > 0) {
      vars.put(
          "VECTOR_COLUMN",
          new StringSubstitutor(Map.of("VECTOR_DIM", vectorLength))
              .replace(CQL.TABLE_VECTOR_COLUMN_TEMPLATE));
    }

    if (indexAnalyzer != null) {
      vars.put("LEXICAL_COLUMN", CQL.TABLE_LEXICAL_COLUMN_TEMPLATE);
    }

    if (comment != null) {
      vars.put(
          "COMMENT_CLAUSE",
          new StringSubstitutor(Map.of("COMMENT", comment)).replace(CQL.TABLE_COMMENT_CLAUSE_TEMPLATE));
    }

    var result = new StringSubstitutor(vars).replace(CQL.CREATE_TABLE_TEMPLATE);
    return collapseWhitespace ? collapseWhitespace(result) : result;
  }

  private Stream<SuperShreddingComponent<String>> indexCQL(){
    var defsAndOptions = indexDefsAndOptions();

    // we will have the low-level indexing options, we will need to use those to make the
    // clauses for the indexes the need them.

    var cqlAndDefs = defsAndOptions.indexDefs().stream()
            .map(IndexCQLAndDefs.ALL_INDEXES_BY_INDEX_DEF::get)
            .toList();

    // need to use the options values with the CQL
    Map<String, String> indexVars = new HashMap<>();
    for (IndexCQLAndDef cqlAndDef : cqlAndDefs ) {
      if (cqlAndDef.clauseTemplate() != null){
        // run the template for this clause, blindly get options the builder has
        // null and empty are OK, If we get a clause back, then put that into the index vars
        // e.g. look at LEXICAL_WITH_OPTIONS_TEMPLATE, we add the

        cqlAndDef.clauseTemplate()
                .format(defsAndOptions.indexOptions().get(cqlAndDef.indexDef()))
                .map(clause -> indexVars.put(cqlAndDef.clauseTemplate().toKeyName(), clause));
      }
    }
    // using internal the keyspace and table names because the collection name is
    // used as part of the index name, so we dont want quotes on them
    // templates needs to put the quotes on
    if (ifNotExists) {
      indexVars.put("IF_NOT_EXISTS", "IF NOT EXISTS");
    }

    indexVars.put("KEYSPACE", keyspace.asInternal());
    indexVars.put("TABLE", collection.asInternal());
    var substitutor = new StringSubstitutor(indexVars);

    return cqlAndDefs.stream()
            .map(cqlAndDef -> {
              var cql = substitutor.replace(cqlAndDef.cql());

              return new SuperShreddingComponent<>(
                      cqlAndDef.indexDef().indexName(collection),
                      SuperShreddingComponentType.INDEX,
                      collapseWhitespace ? collapseWhitespace(cql) : cql);
            });

  }


}
