package com.highergear.beam.sample.schema.builder;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class FieldSchemaListBuilder {

    public static final String INTEGER = "INTEGER";
    public static final String STRING = "STRING";
    public static final String TIMESTAMP = "TIMESTAMP";
    public static final String BOOLEAN = "BOOLEAN";
    public static final String RECORD = "RECORD";
    public static final String REQUIRED = "REQUIRED";
    public static final String NULLABLE = "NULLABLE";
    public static final String REPEATED = "REPEATED";

    final List<TableFieldSchema> schemaFields = new ArrayList<>();

    /**
     * Returns a TableSchema for this list of fields.
     *
     * @return the BigQuery TableSchema object for this list of fields
     */
    public TableSchema schema() {
        TableSchema result = new TableSchema();
        result.setFields(schemaFields);
        return result;
    }

    /**
     * Convenience method for builder that constructs an INTEGER type field and adds it to the
     * FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @param description a description of the field
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder intField(String name, String mode, String description) {
        schemaFields.add(fieldSchema(INTEGER, name, mode, description));
        return this;
    }

    /**
     * Convenience method for builder that constructs an INTEGER type field with an empty description
     * and adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder intField(String name, String mode) {
        return intField(name, mode, "");
    }

    /**
     * Convenience method for builder that constructs an INTEGER type field with an empty description
     * and Nullable mode adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder intField(String name) {
        return intField(name, NULLABLE);
    }

    /**
     * Creates a TableFieldSchema with all the parameters
     *
     * @param type - the datatype @see https://cloud.google.com/bigquery/data-types
     * @param name - the name of the field
     * @param mode - the mode of the field
     * @param description - a description of the field to create.
     * @see TableFieldSchema * @return
     */
    public TableFieldSchema fieldSchema(String type, String name, String mode, String description) {
        TableFieldSchema tfs = new TableFieldSchema();
        tfs.setType(type);
        tfs.setName(name);
        tfs.setMode(mode);
        tfs.setDescription(description);
        return tfs;
    }

    /**
     * Convenience method for builder that constructs an STRING type field with the given parameters
     * adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @param description the description for the field.
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder stringField(String name, String mode, String description) {
        schemaFields.add(fieldSchema(STRING, name, mode, description));
        return this;
    }

    /**
     * Convenience method for builder that constructs an STRING type field with the given parameters
     * adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder stringField(String name, String mode) {
        return stringField(name, mode, "");
    }

    /**
     * Convenience method for builder that constructs an STRING type field with the given parameters
     * adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder stringField(String name) {
        return stringField(name, NULLABLE);
    }

    /**
     * Convenience method for builder that constructs an BOOLEAN type field with the given parameters
     * adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @param description the description for the field.
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder boolField(String name, String mode, String description) {
        schemaFields.add(fieldSchema(BOOLEAN, name, mode, description));
        return this;
    }
    /**
     * Convenience method for builder that constructs a Boolean type field with the given name and
     * NULLABLE mode adds it to the builder's list and returns the builder.
     *
     * @param name - name of the field.
     * @param mode - the mode for the field.
     * @see TableFieldSchema * @return this
     */
    public FieldSchemaListBuilder boolField(String name, String mode) {
        return boolField(name, mode, "");
    }

    /**
     * Convenience method for builder that constructs a Boolean type field with the given name and
     * NULLABLE mode adds it to the builder's list and returns the builder.
     *
     * @param name - the name of the field
     * @see TableFieldSchema
     * @return this
     */
    public FieldSchemaListBuilder boolField(String name) {
        return boolField(name, NULLABLE);
    }

    /**
     * Convenience method for builder that constructs a Timestamp type field with the given parameters
     * adds it to the FieldSchemaListBuilder's list of fields.
     *
     * @param name - the name of the field
     * @param mode the mode of the field
     * @param description the description for the field.
     * @see TableFieldSchema
     * @return this
     */
    public FieldSchemaListBuilder timestampField(String name, String mode, String description) {
        schemaFields.add(fieldSchema(TIMESTAMP, name, mode, description));
        return this;
    }

    /**
     * Adds a timestamp field to the builder's list with the given name and mode and returns the
     * builder.
     *
     * @param name - name of the field
     * @param mode - mode for the timestamp field
     * @see TableFieldSchema
     * @return this
     */
    public FieldSchemaListBuilder timestampField(String name, String mode) {
        return timestampField(name, mode, "");
    }

    /**
     * Creates a timestampField with a NULLABLE mode adds it to the builder's list and returns the
     * builder
     *
     * @param name - name of the TableFieldSchema
     * @return this
     */
    public FieldSchemaListBuilder timestampField(String name) {
        return timestampField(name, NULLABLE);
    }
}
