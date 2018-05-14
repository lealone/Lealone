package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
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
import org.lealone.orm.property.TQProperty;
import org.lealone.test.generated.model.AllType.AllTypeDeserializer;

/**
 * Model for table 'ALL_TYPE'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = AllTypeDeserializer.class)
public class AllType extends Model<AllType> {

    public static final AllType dao = new AllType(null, true);

    public static AllType create(String url) {
        ModelTable t = new ModelTable(url, "ALL_TYPE");
        return new AllType(t);
    }

    public final PInteger<AllType> f1;
    public final PBoolean<AllType> f2;
    public final PByte<AllType> f3;
    public final PShort<AllType> f4;
    public final PLong<AllType> f5;
    public final PLong<AllType> f6;
    public final PBigDecimal<AllType> f7;
    public final PDouble<AllType> f8;
    public final PFloat<AllType> f9;
    public final PTime<AllType> f10;
    public final PDate<AllType> f11;
    public final PTimestamp<AllType> f12;
    public final PBytes<AllType> f13;
    public final PObject<AllType> f14;
    public final PString<AllType> f15;
    public final PString<AllType> f16;
    public final PString<AllType> f17;
    public final PBlob<AllType> f18;
    public final PClob<AllType> f19;
    public final PUuid<AllType> f20;
    public final PArray<AllType> f21;

    public AllType() {
        this(null);
    }

    private AllType(ModelTable t) {
        this(t, false);
    }

    private AllType(ModelTable t, boolean isDao) {
        super(t == null ? new ModelTable("ALL_TYPE") : t, isDao);
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
        super.setTQProperties(new TQProperty[] { this.f1, this.f2, this.f3, this.f4, this.f5, this.f6, this.f7, this.f8, this.f9, this.f10, this.f11, this.f12, this.f13, this.f14, this.f15, this.f16, this.f17, this.f18, this.f19, this.f20, this.f21 });
    }

    @Override
    protected AllType newInstance(ModelTable t) {
        return new AllType(t);
    }

    static class AllTypeDeserializer extends ModelDeserializer<AllType> {
        @Override
        protected Model<AllType> newModelInstance() {
            return new AllType();
        }
    }
}
