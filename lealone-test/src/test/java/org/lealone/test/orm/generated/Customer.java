package org.lealone.test.orm.generated;

import org.lealone.orm.Table;

/**
 * Model bean for table 'CUSTOMER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class Customer {

    public static Customer create(String url) {
        Table t = new Table(url, "CUSTOMER");
        return new Customer(t);
    }

    private Table _t_;

    private Long id;
    private String name;
    private String notes;
    private Integer phone;

    public Customer() {
    }

    private Customer(Table t) {
        this._t_ = t;
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

    public void save() {
        _t_.save(this);
    }

    public boolean delete() {
       return _t_.delete(this);
    }
}
