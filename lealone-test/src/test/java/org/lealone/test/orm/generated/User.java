package org.lealone.test.orm.generated;

import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PArray;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PString;

/**
 * Model for table 'USER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class User extends Model<User> {

    public static final User dao = new User(null, ROOT_DAO);

    public final PString<User> name;
    public final PString<User> notes;
    public final PInteger<User> phone;
    public final PLong<User> id;
    public final PArray<User> phones;

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
        this.phones = new PArray<>("PHONES", this);
        super.setModelProperties(new ModelProperty[] { this.name, this.notes, this.phone, this.id, this.phones });
    }

    @Override
    protected User newInstance(ModelTable t, short modelType) {
        return new User(t, modelType);
    }

    public static User decode(String str) {
        return new User().decode0(str);
    }
}
