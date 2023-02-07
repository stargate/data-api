package io.stargate.sgv2.jsonapi.service.sequencer;

/** Query options passed to each sequence part. */
public class QueryOptions {

  /** Type of the query. */
  public enum Type {
    WRITE,
    READ,
    SCHEMA
  }

  private final Type type;
  private Integer pageSize;
  private String pagingState;

  /** Default constructor that defines the type. */
  public QueryOptions(Type type) {
    if (null == type) {
      throw new IllegalArgumentException("Type of the query must be defined.");
    }
    this.type = type;
  }

  public Type getType() {
    return type;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  public String getPagingState() {
    return pagingState;
  }

  /**
   * Sets page size of the query/queries. Note that this option is ignored if query {@link #type} is
   * not {@link Type#READ}.
   */
  void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

  /**
   * Sets page state of the query/queries. Note that this option is ignored if query {@link #type}
   * is not {@link Type#READ}.
   *
   * <p><b>WARNING:</b> this should be used only when constructing the {@link SingleQuerySequence}.
   */
  void setPagingState(String pagingState) {
    this.pagingState = pagingState;
  }
}
