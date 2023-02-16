package com.highergear.beam.sample.options;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AWSToBQETLOptions extends PipelineOptions, S3Options {

    @Description("Big Query table name")
    @Default.String("sample_etl_output")
    String getBigQueryTableName();

    void setBigQueryTableName(String tableName);

    @Description("Bucket location for the input data in the format gs://<BUCKET_NAME> or s3://<BUCKET_NAME>")
    @Default.String("gs://etl_playground_1")
    String getInputBucketUrl();

    void setInputBucketUrl(String inputBucketUrl);

    @Description("AWS S3 access key ID")
    @Default.String("")
    String getAwsAccessKey();

    void setAwsAccessKey(String awsAccessKey);

    @Description("AWS S3 secret key ID")
    @Default.String("")
    String getAwsSecretKey();

    void setAwsSecretKey(String awsSecretKey);
}
