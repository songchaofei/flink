package com.scf.beans;

import lombok.*;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserBean {
    private int id;
    private String name;
    private int age;
    private String city;
    private Long birthday;
}
