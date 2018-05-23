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

    public static final User dao = new User(null, ROOT_DAO);

    public final PString<User> name;
    public final PString<User> notes;
    public final PInteger<User> phone;
    public final PLong<User> id;

    public User() {
        this(null, REGULAR_MODEL);
    }

    private User(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "USER") : t, modelType);
        super.setRoot(this);

        this.name = new PString<>("NAME", this);
        this.notes = new PString<>("NOTES", this);
        this.phone = new PInteger<>("PHONE", this);
        this.id = new PLong<>("ID", this);
        super.setModelProperties(new ModelProperty[] { this.name, this.notes, this.phone, this.id });
    }

    @Override
    protected User newInstance(ModelTable t, short modelType) {
        return new User(t, modelType);
    }

    static class UserDeserializer extends ModelDeserializer<User> {
        @Override
        protected Model<User> newModelInstance() {
            return new User();
        }
    }
}
