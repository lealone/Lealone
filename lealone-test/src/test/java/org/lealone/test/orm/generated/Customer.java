package org.lealone.test.orm.generated;

/**
 * Model bean for table 'CUSTOMER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class Customer {
    private Long id;
    private String name;
    private String notes;
    private Integer phone;

    public Customer() {
    }

    public Customer setId(Long id) {
        this.id = id; 
        return this;
    }

    public Long getId() { 
        return id; 
    }

    public Customer setName(String name) {
        this.name = name; 
        return this;
    }

    public String getName() { 
        return name; 
    }

    public Customer setNotes(String notes) {
        this.notes = notes; 
        return this;
    }

    public String getNotes() { 
        return notes; 
    }

    public Customer setPhone(Integer phone) {
        this.phone = phone; 
        return this;
    }

    public Integer getPhone() { 
        return phone; 
    }
}
