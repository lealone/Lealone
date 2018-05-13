package org.lealone.test.orm.generated;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Query;
import org.lealone.orm.QueryDeserializer;
import org.lealone.orm.QuerySerializer;
import org.lealone.orm.Table;
import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PLong;
import org.lealone.orm.typequery.PString;
import org.lealone.orm.typequery.TQProperty;
import org.lealone.test.orm.generated.Customer.CustomerDeserializer;

/**
 * Model for table 'CUSTOMER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = QuerySerializer.class)
@JsonDeserialize(using = CustomerDeserializer.class)
public class Customer extends Query<Customer> {

    public static final Customer dao = new Customer(null, true);

    public static Customer create(String url) {
        Table t = new Table(url, "CUSTOMER");
        return new Customer(t);
    }

    public final PLong<Customer> id;
    public final PString<Customer> name;
    public final PString<Customer> notes;
    public final PInteger<Customer> phone;

    public Customer() {
        this(null, false);
    }

    public Customer(Table t) {
        this(t, false);
    }

    private Customer(Table t, boolean isDao) {
        super(t, "CUSTOMER", isDao);
        super.setRoot(this);

        this.id = new PLong<>("ID", this);
        this.name = new PString<>("NAME", this);
        this.notes = new PString<>("NOTES", this);
        this.phone = new PInteger<>("PHONE", this);
        super.setTQProperties(new TQProperty[] { this.id, this.name, this.notes, this.phone });
    }

    @Override
    protected Customer newInstance(Table t) {
        return new Customer(t);
    }

    static class CustomerDeserializer extends QueryDeserializer<Customer> {
        @Override
        protected Query<Customer> newQueryInstance() {
            return new Customer();
        }
    }
}
