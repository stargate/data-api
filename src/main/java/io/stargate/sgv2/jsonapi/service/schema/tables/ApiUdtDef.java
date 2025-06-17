package io.stargate.sgv2.jsonapi.service.schema.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.resolver.VectorizeConfigValidator;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.*;

/**
 * The API model for a user-defined type(UDT) in the database.
 *
 * <p>Created by the factories {@link #FROM_TYPE_DESC_FACTORY} and TODO FROM CQL.
 *
 * <p>Note, call it ApiUdtDef instead of ApiTypeDef to avoid confusion with the regular data types.
 */
public class ApiUdtDef implements Recordable {

  public static final ApiUdtDef.FromTypeDescFactory FROM_TYPE_DESC_FACTORY =
      new ApiUdtDef.FromTypeDescFactory();

  private final CqlIdentifier name;
  private final ApiColumnDefContainer allFields;

  private ApiUdtDef(CqlIdentifier name, ApiColumnDefContainer allFields) {
    this.name = name;
    this.allFields = allFields;
  }

  /** Get all fields in the UDT. */
  public ApiColumnDefContainer allFields() {
    return allFields;
  }

  /** Get the UDT name cqlIdentifier. */
  public CqlIdentifier name() {
    return name;
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder.append("name", name).append("allColumns", allFields);
  }

  /**
   * Factory for creating a {@link ApiUdtDef} from user description sent in a command {@link
   * io.stargate.sgv2.jsonapi.api.model.command.table.definition.TypeDefinitionDesc}.
   *
   * <p>Use the singleton {@link #FROM_TYPE_DESC_FACTORY} to create an instance.
   */
  public static final class FromTypeDescFactory extends FactoryFromDesc {

    FromTypeDescFactory() {}

    public ApiUdtDef create(
        String name,
        TypeDefinitionDesc typeDefinitionDesc,
        VectorizeConfigValidator validateVectorize) {

      Objects.requireNonNull(typeDefinitionDesc, "typeDefinitionDesc must not be null");

      var udtIdentifier = userNameToIdentifier(name, "udtName");

      // user must provide at least one field in the UDT
      if (typeDefinitionDesc.fields() == null || typeDefinitionDesc.fields().isEmpty()) {
        throw SchemaException.Code.MISSING_FIELDS_FOR_TYPE_CREATION.get();
      }

      // passing in validateVectorize since columnDefs need it.
      var allColumnDefs =
          ApiColumnDefContainer.FROM_COLUMN_DESC_FACTORY.create(
              typeDefinitionDesc.fields(), validateVectorize);

      return new ApiUdtDef(udtIdentifier, allColumnDefs);
    }
  }
}
