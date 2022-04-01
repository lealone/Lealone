package org.lealone.test.orm.generated;

import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PString;

/**
 * Model for table 'CUSTOMER_ADDRESS'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class CustomerAddress extends Model<CustomerAddress> {

    public static final CustomerAddress dao = new CustomerAddress(null, ROOT_DAO);

    public final PLong<CustomerAddress> customerId;
    public final PString<CustomerAddress> city;
    public final PString<CustomerAddress> street;
    private Customer customer;

    public CustomerAddress() {
        this(null, REGULAR_MODEL);
    }

    private CustomerAddress(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "CUSTOMER_ADDRESS") : t, modelType);
        super.setRoot(this);

        this.customerId = new PLong<>("CUSTOMER_ID", this);
        this.city = new PString<>("CITY", this);
        this.street = new PString<>("STREET", this);
        super.setModelProperties(new ModelProperty[] { this.customerId, this.city, this.street });
    }

    public Customer getCustomer() {
        return customer;
    }

    public CustomerAddress setCustomer(Customer customer) {
        this.customer = customer;
        this.customerId.set(customer.id.get());
        return this;
    }

    @Override
    protected CustomerAddress newInstance(ModelTable t, short modelType) {
        return new CustomerAddress(t, modelType);
    }

    public static CustomerAddress decode(String str) {
        return new CustomerAddress().decode0(str);
    }
}
