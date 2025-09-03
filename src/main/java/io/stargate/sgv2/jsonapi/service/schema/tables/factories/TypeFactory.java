package io.stargate.sgv2.jsonapi.service.schema.tables.factories;

import static io.stargate.sgv2.jsonapi.exception.ErrorFormatters.errFmt;
import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.ColumnDesc;

/** Base for Factories that create api "Def" objects from the user inputted "Desc" objects. */
public abstract class TypeFactory {

  /**
   * Constant to use when a user described type is missing a component, such as a map missing a key.
   */
  protected static final String MISSING_TYPE = "[MISSING]";

  /**
   * if the <code>columnDesc</code> is null, returns {@link TypeFactory#MISSING_TYPE} otherwise
   * normal error formatting. Normal error formatting would output <code>"null"</code> which could
   * be mistaken for JSON `null`.
   */
  protected static String errFmtOrMissing(ColumnDesc columnDesc) {
    return columnDesc == null ? MISSING_TYPE : errFmt(columnDesc);
  }

  protected CqlIdentifier userNameToIdentifier(String userName, String context) {
    if (userName == null) {
      throw new IllegalArgumentException("%s is must not be null".formatted(context));
    }
    if (userName.isBlank()) {
      throw new IllegalArgumentException("%s is must not be blank".formatted(context));
    }
    return cqlIdentifierFromUserInput(userName);
  }

  /**
   * Checks that the given superObject is an instance of the childClass, and casts it to childClass.
   */
  protected <SuperT, ChildT extends SuperT> ChildT checkCastToChild(
      String context, Class<ChildT> childClass, SuperT superObject) {
    if (!childClass.isInstance(superObject)) {
      throw new IllegalArgumentException(
          "%s - super object is not an instance of child, childClass: %s, superObjects.class: %s"
              .formatted(
                  context,
                  childClass.getName(),
                  superObject == null ? "null" : superObject.getClass()));
    }
    return childClass.cast(superObject);
  }
}
