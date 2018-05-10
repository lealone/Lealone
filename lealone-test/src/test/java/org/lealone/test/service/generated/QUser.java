package org.lealone.test.service.generated;

import org.lealone.orm.Query;
import org.lealone.orm.Table;

import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PLong;
import org.lealone.orm.typequery.PString;

/**
 * Query bean for model 'User'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class QUser extends Query<User, QUser> {

    public static QUser create(String url) {
        Table t = new Table(url, "USER");
        return new QUser(t);
    }

    public final PLong<QUser> id;
    public final PString<QUser> name;
    public final PString<QUser> notes;
    public final PInteger<QUser> phone;

    private QUser(Table t) {
        super(t);
        setRoot(this);

        this.id = new PLong<>("id", this);
        this.name = new PString<>("name", this);
        this.notes = new PString<>("notes", this);
        this.phone = new PInteger<>("phone", this);
    }
}
