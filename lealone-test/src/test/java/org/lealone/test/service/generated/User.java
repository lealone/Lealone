package org.lealone.test.service.generated;

import org.lealone.orm.Table;

/**
 * Model bean for table 'USER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class User {

    public static User create(String url) {
        Table t = new Table(url, "USER");
        return new User(t);
    }

    private Table _t_;

    private Long id;
    private String name;
    private String notes;
    private Integer phone;

    public User() {
    }

    private User(Table t) {
        this._t_ = t;
    }

    public User setId(Long id) {
        this.id = id; 
        return this;
    }

    public Long getId() { 
        return id; 
    }

    public User setName(String name) {
        this.name = name; 
        return this;
    }

    public String getName() { 
        return name; 
    }

    public User setNotes(String notes) {
        this.notes = notes; 
        return this;
    }

    public String getNotes() { 
        return notes; 
    }

    public User setPhone(Integer phone) {
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
