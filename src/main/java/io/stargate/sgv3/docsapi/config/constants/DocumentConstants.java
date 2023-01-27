package io.stargate.sgv3.docsapi.config.constants;

public interface DocumentConstants {
  /** Names of "special" fields in Documents */
  interface Fields {
    /** Primary key for Documents stored; has special handling for many operations. */
    String DOC_ID = "_id";
  }
}
