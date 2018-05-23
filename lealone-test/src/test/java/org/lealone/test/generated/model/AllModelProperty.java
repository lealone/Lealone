package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PArray;
import org.lealone.orm.property.PBigDecimal;
import org.lealone.orm.property.PBlob;
import org.lealone.orm.property.PBoolean;
import org.lealone.orm.property.PByte;
import org.lealone.orm.property.PBytes;
import org.lealone.orm.property.PClob;
import org.lealone.orm.property.PDate;
import org.lealone.orm.property.PDouble;
import org.lealone.orm.property.PFloat;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PObject;
import org.lealone.orm.property.PShort;
import org.lealone.orm.property.PString;
import org.lealone.orm.property.PTime;
import org.lealone.orm.property.PTimestamp;
import org.lealone.orm.property.PUuid;
import org.lealone.test.generated.model.AllModelProperty.AllModelPropertyDeserializer;

/**
 * Model for table 'ALL_MODEL_PROPERTY'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = AllModelPropertyDeserializer.class)
public class AllModelProperty extends Model<AllModelProperty> {

    public static final AllModelProperty dao = new AllModelProperty(null, ROOT_DAO);

    public final PInteger<AllModelProperty> f1;
    public final PBoolean<AllModelProperty> f2;
    public final PByte<AllModelProperty> f3;
    public final PShort<AllModelProperty> f4;
    public final PLong<AllModelProperty> f5;
    public final PLong<AllModelProperty> f6;
    public final PBigDecimal<AllModelProperty> f7;
    public final PDouble<AllModelProperty> f8;
    public final PFloat<AllModelProperty> f9;
    public final PTime<AllModelProperty> f10;
    public final PDate<AllModelProperty> f11;
    public final PTimestamp<AllModelProperty> f12;
    public final PBytes<AllModelProperty> f13;
    public final PObject<AllModelProperty> f14;
    public final PString<AllModelProperty> f15;
    public final PString<AllModelProperty> f16;
    public final PString<AllModelProperty> f17;
    public final PBlob<AllModelProperty> f18;
    public final PClob<AllModelProperty> f19;
    public final PUuid<AllModelProperty> f20;
    public final PArray<AllModelProperty> f21;

    public AllModelProperty() {
        this(null, REGULAR_MODEL);
    }

    private AllModelProperty(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "ALL_MODEL_PROPERTY") : t, modelType);
        super.setRoot(this);

        this.f1 = new PInteger<>("F1", this);
        this.f2 = new PBoolean<>("F2", this);
        this.f3 = new PByte<>("F3", this);
        this.f4 = new PShort<>("F4", this);
        this.f5 = new PLong<>("F5", this);
        this.f6 = new PLong<>("F6", this);
        this.f7 = new PBigDecimal<>("F7", this);
        this.f8 = new PDouble<>("F8", this);
        this.f9 = new PFloat<>("F9", this);
        this.f10 = new PTime<>("F10", this);
        this.f11 = new PDate<>("F11", this);
        this.f12 = new PTimestamp<>("F12", this);
        this.f13 = new PBytes<>("F13", this);
        this.f14 = new PObject<>("F14", this);
        this.f15 = new PString<>("F15", this);
        this.f16 = new PString<>("F16", this);
        this.f17 = new PString<>("F17", this);
        this.f18 = new PBlob<>("F18", this);
        this.f19 = new PClob<>("F19", this);
        this.f20 = new PUuid<>("F20", this);
        this.f21 = new PArray<>("F21", this);
        super.setModelProperties(new ModelProperty[] { this.f1, this.f2, this.f3, this.f4, this.f5, this.f6, this.f7, this.f8, this.f9, this.f10, this.f11, this.f12, this.f13, this.f14, this.f15, this.f16, this.f17, this.f18, this.f19, this.f20, this.f21 });
    }

    @Override
    protected AllModelProperty newInstance(ModelTable t, short modelType) {
        return new AllModelProperty(t, modelType);
    }

    static class AllModelPropertyDeserializer extends ModelDeserializer<AllModelProperty> {
        @Override
        protected Model<AllModelProperty> newModelInstance() {
            return new AllModelProperty();
        }
    }
}
