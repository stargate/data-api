package io.stargate.sgv3.docsapi.commands.clauses;

/** Common projection clause used in many commands */
public class ProjectionClause extends Clause {
  /** If there is no projection clause specified then we will selected All fields from the doc. */
  public static final ProjectionClause ALL = new ProjectionClause();

  public interface Projectable {
    ProjectionClause getProjection();
  }
}
