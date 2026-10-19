package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

/**
 * If a findAndRerank command reads with one leg or with more than one, decided by its sort clause:
 * {@link #MULTI_LEG} when the sort clause uses $hybrid, otherwise {@link #SINGLE_LEG}.
 *
 * <p>A MULTI_LEG command may still read with only one leg, e.g. <code>{"$hybrid": "cheese"}</code>
 * on a collection without a lexical index.
 */
public enum RerankLegMode {
  SINGLE_LEG,
  MULTI_LEG
}
