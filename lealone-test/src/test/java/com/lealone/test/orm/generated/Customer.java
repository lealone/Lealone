package com.lealone.test.orm.generated;

import com.lealone.orm.Model;
import com.lealone.orm.ModelProperty;
import com.lealone.orm.ModelTable;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.property.PInteger;
import com.lealone.orm.property.PLong;
import com.lealone.orm.property.PString;
import java.util.List;

/**
 * Model for table 'CUSTOMER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class Customer extends Model<Customer> {

    public static final Customer dao = new Customer(null, ROOT_DAO);

    public final PLong<Customer> id;
    public final PString<Customer> name;
    public final PString<Customer> notes;
    public final PInteger<Customer> phone;

    public Customer() {
        this(null, REGULAR_MODEL);
    }

    private Customer(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "CUSTOMER") : t, modelType);
        id = new PLong<>("ID", this);
        name = new PString<>("NAME", this);
        notes = new PString<>("NOTES", this);
        phone = new PInteger<>("PHONE", this);
        super.setModelProperties(new ModelProperty[] { id, name, notes, phone });
        super.initAdders(new CustomerAddressAdder(), new OrderAdder());
    }

    @Override
    protected Customer newInstance(ModelTable t, short modelType) {
        return new Customer(t, modelType);
    }

    public Customer addCustomerAddress(CustomerAddress m) {
        m.setCustomer(this);
        super.addModel(m);
        return this;
    }

    public Customer addCustomerAddress(CustomerAddress... mArray) {
        for (CustomerAddress m : mArray)
            addCustomerAddress(m);
        return this;
    }

    public List<CustomerAddress> getCustomerAddressList() {
        return super.getModelList(CustomerAddress.class);
    }

    public Customer addOrder(Order m) {
        m.setCustomer(this);
        super.addModel(m);
        return this;
    }

    public Customer addOrder(Order... mArray) {
        for (Order m : mArray)
            addOrder(m);
        return this;
    }

    public List<Order> getOrderList() {
        return super.getModelList(Order.class);
    }

    protected class CustomerAddressAdder implements AssociateAdder<CustomerAddress> {
        @Override
        public CustomerAddress getDao() {
            return CustomerAddress.dao;
        }

        @Override
        public void add(CustomerAddress m) {
            if (areEqual(id, m.customerId)) {
                addCustomerAddress(m);
            }
        }
    }

    protected class OrderAdder implements AssociateAdder<Order> {
        @Override
        public Order getDao() {
            return Order.dao;
        }

        @Override
        public void add(Order m) {
            if (areEqual(id, m.customerId)) {
                addOrder(m);
            }
        }
    }

    public static Customer decode(String str) {
        return decode(str, null);
    }

    public static Customer decode(String str, JsonFormat format) {
        return new Customer().decode0(str, format);
    }
}
