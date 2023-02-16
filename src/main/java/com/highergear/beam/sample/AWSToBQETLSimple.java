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
                                                .set(schemaFields.get(0).getName(), customer.getCustomerId())
                                                .set(schemaFields.get(1).getName(), customer.isNameStyle())
                                                .set(schemaFields.get(2).getName(), customer.getTitle())
                                                .set(schemaFields.get(3).getName(), customer.getFirstName())
                                                .set(schemaFields.get(4).getName(), customer.getMiddleName())
                                                .set(schemaFields.get(5).getName(), customer.getLastName())
                                                .set(schemaFields.get(6).getName(), customer.getSuffix())
                                                .set(schemaFields.get(7).getName(), customer.getCompanyName())
                                                .set(schemaFields.get(8).getName(), customer.getSalesPerson())
                                                .set(schemaFields.get(9).getName(), customer.getEmailAddress())
                                                .set(schemaFields.get(10).getName(), customer.getPhone())
                                                .set(schemaFields.get(11).getName(), customer.getPasswordHash())
                                                .set(schemaFields.get(12).getName(), customer.getPasswordSalt())
                                                .set(schemaFields.get(13).getName(), customer.getRowGuid())
                                                .set(schemaFields.get(14).getName(), customer.getModifiedDate()))
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run().waitUntilFinish();
    }
}
