package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import io.stargate.sgv2.jsonapi.service.schema.tables.BindingPointRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.TypeBindingPoint;

/** These rules are only used to know if we should create a frozen UDT when binding the type */
public class FrozenUdtBindingRules
    extends BindingPointRules<FrozenUdtBindingRules.FrozenUdtBindingRule> {

  FrozenUdtBindingRules() {
    super(
        new FrozenUdtBindingRule(TypeBindingPoint.COLLECTION_VALUE, true),
        new FrozenUdtBindingRule(TypeBindingPoint.MAP_KEY, false),
        new FrozenUdtBindingRule(TypeBindingPoint.TABLE_COLUMN, false),
        new FrozenUdtBindingRule(TypeBindingPoint.UDT_FIELD, false));
  }

  //  ColumnDescBindingPointRules() {
  //    super(
  //        new ColumnDescBindingPointRule(
  //            TypeBindingPoint.COLLECTION_VALUE,
  //            false,
  //            false,
  //            true,
  //            true,
  //            errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs())
  //                + DELIMITER
  //                + ApiTypeName.UDT.apiName()),
  //        new ColumnDescBindingPointRule(
  //            TypeBindingPoint.MAP_KEY,
  //            false,
  //            false,
  //            false,
  //            false,
  //            errFmtColumnDesc(PrimitiveColumnDesc.allColumnDescs())),
  //        new ColumnDescBindingPointRule(
  //            TypeBindingPoint.TABLE_COLUMN,
  //            true,
  //            true,
  //            true,
  //            true,
  //            errFmtJoin(ApiTypeName.all(), ApiTypeName::apiName)),
  //        new ColumnDescBindingPointRule(
  //            TypeBindingPoint.UDT_FIELD,
  //            false,
  //            false,
  //            false,
  //            false,
  //            errFmtJoin(PrimitiveColumnDesc.allColumnDescs(), PrimitiveColumnDesc::apiName)));
  //  }

  public record FrozenUdtBindingRule(
      TypeBindingPoint bindingPoint,
      /** True the UDT should be frozen at this binding point */
      boolean useFrozenUdt)
      implements BindingPointRule {

    //    public void checkAllowCollectionTypes(ApiTypeName apiTypeName) {
    //      if (!allowCollectionTypes) {
    //        throwSchemaException(apiTypeName);
    //      }
    //    }
    //
    //    public void checkAllowVectorType(ApiTypeName apiTypeName) {
    //      if (!allowVectorType) {
    //        throwSchemaException(apiTypeName);
    //      }
    //    }
    //
    //    public void checkAllowUdtType(ApiTypeName apiTypeName) {
    //      if (!allowUdtType) {
    //        throwSchemaException(apiTypeName);
    //      }
    //    }
    //
    //    private void throwSchemaException(ApiTypeName apiTypeName) {
    //
    //      // we are deserializing UDT fields, which only support primitive types
    //      // from here we know this is not a primitive type, so we can throw an error
    //      throw SchemaException.Code.UNSUPPORTED_TYPE_FIELD.get(
    //          Map.of(
    //              "supportedTypes", supportedTypesMessage, "unsupportedType",
    // apiTypeName.apiName()));
    //    }
  }
}
