package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.List;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PString;
import org.lealone.test.generated.model.Customer.CustomerDeserializer;

/**
 * Model for table 'CUSTOMER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = CustomerDeserializer.class)
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
        super.setRoot(this);

        this.id = new PLong<>("ID", this);
        this.name = new PString<>("NAME", this);
        this.notes = new PString<>("NOTES", this);
        this.phone = new PInteger<>("PHONE", this);
        super.setModelProperties(new ModelProperty[] { this.id, this.name, this.notes, this.phone });
    }

    public Customer addCustomerAddress(CustomerAddress m) {
        m.setCustomer(this);
        super.addModel(m);;
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
        super.addModel(m);;
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

    @Override
    protected Customer newInstance(ModelTable t, short modelType) {
        return new Customer(t, modelType);
    }

    @Override
    protected List<Model<?>> newAssociateInstances() {
        ArrayList<Model<?>> list = new ArrayList<>();
        CustomerAddress m1 = new CustomerAddress();
        addCustomerAddress(m1);
        list.add(m1);
        Order m2 = new Order();
        addOrder(m2);
        list.add(m2);
        return list;
    }

    static class CustomerDeserializer extends ModelDeserializer<Customer> {
        @Override
        protected Model<Customer> newModelInstance() {
            return new Customer();
        }
    }
}
