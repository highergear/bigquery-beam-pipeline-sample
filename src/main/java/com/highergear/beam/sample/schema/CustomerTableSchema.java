package com.highergear.beam.sample.schema;

import com.google.api.services.bigquery.model.TableSchema;
import com.highergear.beam.sample.schema.builder.FieldSchemaListBuilder;

public class CustomerTableSchema {

    public static TableSchema Create() {
        FieldSchemaListBuilder builder = new FieldSchemaListBuilder();

        return builder
                .intField("customer_id")
                .boolField("name_style")
                .stringField("title")
                .stringField("first_name")
                .stringField("middle_name")
                .stringField("last_name")
                .stringField("suffix")
                .stringField("company_name")
                .stringField("sales_person")
                .stringField("email_address")
                .stringField("phone")
                .stringField("password_hash")
                .stringField("password_salt")
                .stringField("row_guid")
                .stringField("modified_date")
                .schema();
    }
}
