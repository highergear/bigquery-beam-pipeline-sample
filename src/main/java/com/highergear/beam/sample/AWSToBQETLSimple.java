package com.highergear.beam.sample;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.highergear.beam.sample.model.Customer;
import com.highergear.beam.sample.options.AWSToBQETLOptions;
import com.highergear.beam.sample.schema.CustomerTableSchema;
import com.highergear.beam.sample.transform.MapToCustomer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.List;

import static com.highergear.beam.sample.options.AwsOptionsParser.formatOptions;

public class AWSToBQETLSimple {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(AWSToBQETLOptions.class);

        AWSToBQETLOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AWSToBQETLOptions.class);

        formatOptions(options);

        Pipeline p = Pipeline.create(options);

        TableSchema schema = CustomerTableSchema.Create();
        List<TableFieldSchema> schemaFields = schema.getFields();
        String customerIdKey = schemaFields.get(0).getName();
        String nameStyleKey = schemaFields.get(1).getName();
        String titleKey = schemaFields.get(2).getName();
        String firstNameKey = schemaFields.get(3).getName();
        String middleNameKey = schemaFields.get(4).getName();
        String lastNameKey = schemaFields.get(5).getName();
        String suffixKey = schemaFields.get(6).getName();
        String companyNameKey = schemaFields.get(7).getName();
        String salesPersonKey = schemaFields.get(8).getName();
        String emailAddressKey = schemaFields.get(9).getName();
        String phoneKey = schemaFields.get(10).getName();
        String passwordHashKey = schemaFields.get(11).getName();
        String passwordSaltKey = schemaFields.get(12).getName();
        String rowGuidKey = schemaFields.get(13).getName();
        String modifiedDataKey = schemaFields.get(14).getName();

        p.apply("Read Customer",
                        TextIO.read().from(options.getInputBucketUrl() + "customer.csv"))
                .apply("Transform Line", ParDo.of(new MapToCustomer()))
                .apply("Write to Destination Bucket",
                        BigQueryIO
                                .<Customer>write()
                                .to(options.getBigQueryTableName())
                                .withSchema(schema)
                                .withFormatFunction((Customer customer) ->
                                        new TableRow()
                                                .set(customerIdKey, customer.getCustomerId())
                                                .set(nameStyleKey, customer.isNameStyle())
                                                .set(titleKey, customer.getTitle())
                                                .set(firstNameKey, customer.getFirstName())
                                                .set(middleNameKey, customer.getMiddleName())
                                                .set(lastNameKey, customer.getLastName())
                                                .set(suffixKey, customer.getSuffix())
                                                .set(companyNameKey, customer.getCompanyName())
                                                .set(salesPersonKey, customer.getSalesPerson())
                                                .set(emailAddressKey, customer.getEmailAddress())
                                                .set(phoneKey, customer.getPhone())
                                                .set(passwordHashKey, customer.getPasswordHash())
                                                .set(passwordSaltKey, customer.getPasswordSalt())
                                                .set(rowGuidKey, customer.getRowGuid())
                                                .set(modifiedDataKey, customer.getModifiedDate()))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }
}
