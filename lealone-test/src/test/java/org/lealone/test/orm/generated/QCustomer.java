package org.lealone.test.orm.generated;

import org.lealone.orm.Query;
import org.lealone.orm.Table;

import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PLong;
import org.lealone.orm.typequery.PString;

/**
 * Query bean for model 'Customer'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class QCustomer extends Query<Customer, QCustomer> {
    public final PLong<QCustomer> id;
    public final PString<QCustomer> name;
    public final PString<QCustomer> notes;
    public final PInteger<QCustomer> phone;

    public QCustomer(Table t) {
        super(t);
        setRoot(this);

        this.id = new PLong<>("id", this);
        this.name = new PString<>("name", this);
        this.notes = new PString<>("notes", this);
        this.phone = new PInteger<>("phone", this);
    }
}
