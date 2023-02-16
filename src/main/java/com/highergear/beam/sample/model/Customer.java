package com.highergear.beam.sample.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class Customer implements Serializable {
    private int customerId;
    private boolean nameStyle;
    private String title;
    private String firstName;
    private String middleName;
    private String lastName;
    private String suffix;
    private String companyName;
    private String salesPerson;
    private String emailAddress;
    private String phone;
    private String passwordHash;
    private String passwordSalt;
    private String rowGuid;
    private String modifiedDate;
}
