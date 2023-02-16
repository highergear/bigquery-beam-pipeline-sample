package com.highergear.beam.sample;

import com.highergear.beam.sample.model.Customer;
import com.highergear.beam.sample.options.AWSToBQETLOptions;
import com.highergear.beam.sample.schema.CustomerTableSchema;
import com.highergear.beam.sample.transform.MapToCustomer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

import static com.highergear.beam.sample.options.AwsOptionsParser.formatOptions;

public class AWSToBQETLSimple {
    public static void main(String[] args) {
        PipelineOptionsFactory.register(AWSToBQETLOptions.class);

        AWSToBQETLOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(AWSToBQETLOptions.class);

        formatOptions(options);

        Pipeline p = Pipeline.create(options);

        p.apply("Read Customer",
                        TextIO.read().from(options.getInputBucketUrl() + "customer.csv"))
                .apply("Transform Line", ParDo.of(new MapToCustomer()))
                .apply("Write to Destination Bucket",
                        BigQueryIO
                                .<Customer>write()
                                .to(options.getBigQueryTableName())
                                .withSchema(CustomerTableSchema.Create()));

        p.run().waitUntilFinish();
    }
}
