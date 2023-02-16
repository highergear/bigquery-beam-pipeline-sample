package com.highergear.beam.sample.transform;

import com.highergear.beam.sample.model.Customer;
import org.apache.beam.sdk.transforms.DoFn;

public class MapToCustomer extends DoFn<String, Customer> {

    @ProcessElement
    public void processElement(@Element String row, OutputReceiver<Customer> receiver) {
        String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

        if (!fields[0].equals("CustomerID")) {
            receiver.output(
                    new Customer(
                            Integer.parseInt(fields[0]),
                            !fields[1].equals("FALSE"),
                            fields[2],
                            fields[3],
                            fields[4],
                            fields[5],
                            fields[6],
                            fields[7],
                            fields[8],
                            fields[9],
                            fields[10],
                            fields[11],
                            fields[12],
                            fields[13],
                            fields[14]
                            ));
        }
    }
}
