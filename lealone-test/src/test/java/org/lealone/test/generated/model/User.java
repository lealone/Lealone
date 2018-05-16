package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PString;
import org.lealone.test.generated.model.User.UserDeserializer;

/**
 * Model for table 'USER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = UserDeserializer.class)
public class User extends Model<User> {

    public static final User dao = new User(null, true);

    public static User create(String url) {
        ModelTable t = new ModelTable(url, "TEST", "PUBLIC", "USER");
        return new User(t);
    }

    public final PString<User> name;
    public final PString<User> notes;
    public final PInteger<User> phone;
    public final PLong<User> id;

    public User() {
        this(null);
    }

    private User(ModelTable t) {
        this(t, false);
    }

    private User(ModelTable t, boolean isDao) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "USER") : t, isDao);
        super.setRoot(this);

        this.name = new PString<>("NAME", this);
        this.notes = new PString<>("NOTES", this);
        this.phone = new PInteger<>("PHONE", this);
        this.id = new PLong<>("ID", this);
        super.setModelProperties(new ModelProperty[] { this.name, this.notes, this.phone, this.id });
    }

    @Override
    protected User newInstance(ModelTable t) {
        return new User(t);
    }

    static class UserDeserializer extends ModelDeserializer<User> {
        @Override
        protected Model<User> newModelInstance() {
            return new User();
        }
    }
}
