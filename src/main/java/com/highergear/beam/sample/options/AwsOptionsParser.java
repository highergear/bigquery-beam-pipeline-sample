package com.highergear.beam.sample.options;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.io.aws.options.S3Options;

public class AwsOptionsParser {

    // AWS configuration values
    private static final String AWS_DEFAULT_REGION = "ap-southeast-1";
    private static final String AWS_S3_PREFIX = "s3";

    /**
     * Formats BigQueryImportOptions to include AWS specific configuration.
     *
     * @param options for running the Cloud Dataflow pipeline.
     */
    public static void formatOptions(AWSToBQETLOptions options) {
        if (options.getInputBucketUrl().toLowerCase().startsWith(AWS_S3_PREFIX)) {
            setAwsCredentials(options);
        }

        if (options.getAwsRegion() == null) {
            setAwsDefaultRegion(options);
        }
    }

    private static void setAwsCredentials(AWSToBQETLOptions options) {
        options.setAwsCredentialsProvider(
                new AWSStaticCredentialsProvider(
                        new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
    }

    private static void setAwsDefaultRegion(S3Options options) {
        if (options.getAwsRegion() == null) {
            options.setAwsRegion(AWS_DEFAULT_REGION);
        }
    }
}
