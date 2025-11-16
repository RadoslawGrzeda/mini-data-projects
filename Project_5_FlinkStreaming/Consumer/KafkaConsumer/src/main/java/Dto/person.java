package Dto;

import lombok.Data;

@Data
public class person {

    String firstName;
    String lastName;
    String address;
    String email;
    String phoneNumber;
    String dateOfBirth;
    String sex;

    public person() {

    }
    public person(String firstName, String dateOfBirth, String phoneNumber, String email, String address, String lastName, String sex) {
        this.firstName = firstName;
        this.dateOfBirth = dateOfBirth;
        this.phoneNumber = phoneNumber;
        this.email = email;
        this.address = address;
        this.lastName = lastName;
        this.sex = sex;
    }
}
