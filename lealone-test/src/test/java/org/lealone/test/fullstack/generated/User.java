package org.lealone.test.fullstack.generated;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Query;
import org.lealone.orm.QueryDeserializer;
import org.lealone.orm.QuerySerializer;
import org.lealone.orm.Table;
import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PString;
import org.lealone.orm.typequery.TQProperty;
import org.lealone.test.fullstack.generated.User.UserDeserializer;

/**
 * Model for table 'USER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = QuerySerializer.class)
@JsonDeserialize(using = UserDeserializer.class)
public class User extends Query<User> {

    public static final User dao = new User(null, true);

    public static User create(String url) {
        Table t = new Table(url, "USER");
        return new User(t);
    }

    public final PString<User> name;
    public final PString<User> notes;
    public final PInteger<User> phone;

    public User() {
        this(null, false);
    }

    public User(Table t) {
        this(t, false);
    }

    private User(Table t, boolean isDao) {
        super(t, "USER", isDao);
        super.setRoot(this);

        this.name = new PString<>("NAME", this);
        this.notes = new PString<>("NOTES", this);
        this.phone = new PInteger<>("PHONE", this);
        super.setTQProperties(new TQProperty[] { this.name, this.notes, this.phone });
    }

    @Override
    protected User newInstance(Table t) {
        return new User(t);
    }

    static class UserDeserializer extends QueryDeserializer<User> {
        @Override
        protected Query<User> newQueryInstance() {
            return new User();
        }
    }
}
