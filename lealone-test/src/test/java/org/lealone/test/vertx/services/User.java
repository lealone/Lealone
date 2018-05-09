package org.lealone.test.vertx.services;

public class User {
    private Long id;
    private String name;
    private String notes;
    private Integer phone;

    public User() {
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
}
