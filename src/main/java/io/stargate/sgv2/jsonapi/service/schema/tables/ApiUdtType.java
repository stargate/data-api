package io.stargate.sgv2.jsonapi.service.schema.tables;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierFromUserInput;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.stargate.sgv2.jsonapi.api.model.command.table.SchemaDescBindingPoint;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.*;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.exception.checked.UnsupportedCqlType;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorizeDefinition;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UDTCodecs;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.service.schema.naming.NamingRules;
import io.stargate.sgv2.jsonapi.service.schema.tables.factories.*;
import java.util.*;

/**
 * The full representation of a User Defined Type (UDT), that includes the fields and their types.
 * This builds on the {@link ApiUdtShallowType}, this is the type we use when we know the full UDT
 * definition after reading from driver / CQL.
 *
 * <p><b>NOTE:</b> Do not cache instances of this type created from user input via the {@link
 * #FROM_TYPE_DESC_FACTORY} see {@link ApiUdtType#ApiUdtType(CqlIdentifier, boolean,
 * ApiColumnDefContainer)}
 *
 * <p>We make this using the {@link #FROM_CQL_FACTORY} when we read the UDT from the driver schema,
 * there is not a factory to create it from a {@link ColumnDesc} use the {@link
 * ApiUdtShallowType#FROM_COLUMN_DESC_FACTORY} for that.
 *
 * <p>To make an instance that represents the full type for a create or alter command using the
 * special {@link #FROM_TYPE_DESC_FACTORY}
 */
public class ApiUdtType extends ApiUdtShallowType {

  /** Factory to create {@link ApiUdtType} from cqlDataType {@link UserDefinedType}. */
  public static final TypeFactoryFromCql<ApiUdtType, UserDefinedType> FROM_CQL_FACTORY =
      new ApiUdtType.CqlTypeFactory();

  /** Factory to create {@link ApiUdtType} from user provided {@link TypeDefinitionDesc}. */
  public static final TypeDefinitionDescFactory FROM_TYPE_DESC_FACTORY =
      new TypeDefinitionDescFactory();

  /** This is the full driver UDT definition with all fields and their types. */
  private final UserDefinedType fullCqlType;

  private final ApiColumnDefContainer allFields;

  /** For use when we already have the full {@link UserDefinedType} from the driver, */
  private ApiUdtType(UserDefinedType cqlType, ApiColumnDefContainer allFields) {
    super(cqlType.getName(), cqlType.isFrozen());

    this.fullCqlType = Objects.requireNonNull(cqlType, "cqlType must not be null");
    this.allFields = allFields.toUnmodifiable();
  }

  /**
   * For use when we are creating the UDT form user input in a DDL command.
   *
   * <p><b>NOTE:</b> This sets the <code>fullCqlType</code> to <b>null</b> and it will throw and
   * exception if {@link #cqlType()} is called. This is because the {@link UserDefinedTypeBuilder}
   * has some strong warnings about getting the order of the fields in the UDT to match when the DB
   * has. So to be very safe, we will not make a cql type. This object will not be cached, because
   * we do not cache schema objects created from user input.
   */
  private ApiUdtType(CqlIdentifier udtName, boolean isFrozen, ApiColumnDefContainer allFields) {
    super(udtName, isFrozen);

    this.fullCqlType = null;
    this.allFields = allFields.toUnmodifiable();
  }

  @Override
  public ColumnDesc getSchemaDescription(SchemaDescBindingPoint bindingPoint) {

    return switch (bindingPoint) {
      case DDL_USAGE -> // just a reference to the UDT
          new UdtRefColumnDesc(udtName(), isFrozen(), ApiSupportDesc.from(this));
      case DML_USAGE -> // full inline schema desc
          new UdtColumnDesc(
              udtName(),
              isFrozen(),
              allFields.getSchemaDescription(bindingPoint),
              ApiSupportDesc.from(this));
      default -> throw bindingPoint.unsupportedException("ApiUdtType.getSchemaDescription()");
    };
  }

  @Override
  public DataType cqlType() {
    // NOTE: override the shallow type to return the full UDT type
    // See ctor for why this will throw if fullCqlType is null
    if (fullCqlType == null) {
      throw new IllegalStateException(
          "ApiUdtType.cqlType() - unsupported for instances created from user input");
    }
    return fullCqlType;
  }

  public ApiColumnDefContainer allFields() {
    return allFields;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return super.recordTo(dataRecorder).append("allFields", allFields);
  }

  /**
   * Factory to create {@link ApiUdtType} from the description provided by the user.
   *
   * <p>NOTE: This is not implementing the {@link TypeFactoryFromColumnDesc} because that is for the
   * {@link ColumnDesc} which is used when describing a column in a table or UDT. This factory is to
   * create the full {@link ApiUdtType} during a create type command.
   */
  public static class TypeDefinitionDescFactory {

    public ApiUdtType create(
        String udtName,
        TypeDefinitionDesc typeDefinitionDesc,
        VectorizeConfigValidator validateVectorize) {

      var udtIdentifier = cqlIdentifierFromUserInput(NamingRules.UDT.checkRule(udtName));

      // user must provide at least one field in the UDT
      if (typeDefinitionDesc.fields() == null || typeDefinitionDesc.fields().isEmpty()) {
        throw SchemaException.Code.MISSING_FIELDS_FOR_TYPE_CREATION.get();
      }

      var allFields =
          ApiColumnDefContainer.FROM_COLUMN_DESC_FACTORY.create(
              TypeBindingPoint.UDT_FIELD, typeDefinitionDesc.fields(), validateVectorize);

      // never frozen when we are creating the type
      return new ApiUdtType(udtIdentifier, false, allFields);
    }
  }

  /**
   * Factory to create {@link ApiUdtType} from {@link UserDefinedType} obtained from driver / cql.
   *
   * <p>...
   */
  private static final class CqlTypeFactory
      extends TypeFactoryFromCql<ApiUdtType, UserDefinedType> {

    private CqlTypeFactory() {
      super(ProtocolConstants.DataType.UDT, UserDefinedType.class);
    }

    @Override
    public ApiUdtType create(
        TypeBindingPoint bindingPoint, UserDefinedType cqlType, VectorizeDefinition vectorizeDefn)
        throws UnsupportedCqlType {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      if (!isSupported(bindingPoint, cqlType)) {
        throw new UnsupportedCqlType(bindingPoint, cqlType);
      }

      // Fake ColumnMetaData so we can use the ApiColumnDefContainer to describe the fields
      // in the UDT, even though they are not columns it should be ok

      List<ColumnMetadata> fieldsAsMetadata =
          UDTCodecs.udtRawFields(cqlType).stream()
              .map(
                  rawUdtField ->
                      (ColumnMetadata)
                          new ColumnMetadata() {
                            @Override
                            public CqlIdentifier getName() {
                              return rawUdtField.identifier();
                            }

                            @Override
                            public DataType getType() {
                              return rawUdtField.cqlType();
                            }

                            @Override
                            public boolean isStatic() {
                              return false; // UDT fields are not static
                            }

                            @NonNull
                            @Override
                            public CqlIdentifier getKeyspace() {
                              throw new IllegalStateException(
                                  "getKeyspace() should not be called when creating ApiUdtType");
                            }

                            @NonNull
                            @Override
                            public CqlIdentifier getParent() {
                              throw new IllegalStateException(
                                  "getParent() should not be called when creating ApiUdtType");
                            }
                          })
              .toList();

      // TODO: XXX AARON - Confused about the vector configs, but we can never have a vector in UDT,
      // checking if this works
      // if any of the fields are unsupported, we end up with a ApiColumnDef for unsupported type
      var allFields =
          ApiColumnDefContainer.FROM_CQL_FACTORY.create(
              TypeBindingPoint.UDT_FIELD, fieldsAsMetadata, null);

      return new ApiUdtType(cqlType, allFields);
    }

    @Override
    public boolean isSupported(TypeBindingPoint bindingPoint, UserDefinedType cqlType) {
      Objects.requireNonNull(cqlType, "cqlType must not be null");

      //  we accept frozen, but change the support.

      // can we use a UDT of any type in this binding point?
      if (!UDT_BINDING_RULES.rule(bindingPoint).supportedFromDb()) {
        return false;
      }

      // can all the types in the UDT be used in a UDT ?
      for (var rawField : UDTCodecs.udtRawFields(cqlType)) {
        if (!DefaultTypeFactoryFromCql.INSTANCE.isSupportedUntyped(
            TypeBindingPoint.UDT_FIELD, rawField.cqlType())) {
          return false;
        }
      }

      return true;
    }

    @Override
    public Optional<CqlTypeKey> maybeCreateCacheKey(
        TypeBindingPoint bindingPoint, UserDefinedType cqlType) {

      // We cannot cache any UDT type as is user defined  per keyspace / tenant
      return Optional.empty();
    }
  }
}
