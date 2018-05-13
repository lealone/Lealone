package org.lealone.test.orm.generated;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Query;
import org.lealone.orm.QueryDeserializer;
import org.lealone.orm.QuerySerializer;
import org.lealone.orm.Table;
import org.lealone.orm.typequery.PArray;
import org.lealone.orm.typequery.PBigDecimal;
import org.lealone.orm.typequery.PBlob;
import org.lealone.orm.typequery.PBoolean;
import org.lealone.orm.typequery.PByte;
import org.lealone.orm.typequery.PBytes;
import org.lealone.orm.typequery.PClob;
import org.lealone.orm.typequery.PDate;
import org.lealone.orm.typequery.PDouble;
import org.lealone.orm.typequery.PFloat;
import org.lealone.orm.typequery.PInteger;
import org.lealone.orm.typequery.PLong;
import org.lealone.orm.typequery.PObject;
import org.lealone.orm.typequery.PShort;
import org.lealone.orm.typequery.PString;
import org.lealone.orm.typequery.PTime;
import org.lealone.orm.typequery.PTimestamp;
import org.lealone.orm.typequery.PUuid;
import org.lealone.orm.typequery.TQProperty;
import org.lealone.test.orm.generated.AllType.AllTypeDeserializer;

/**
 * Model for table 'ALL_TYPE'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = QuerySerializer.class)
@JsonDeserialize(using = AllTypeDeserializer.class)
public class AllType extends Query<AllType> {

    public static final AllType dao = new AllType();

    public static AllType create(String url) {
        Table t = new Table(url, "ALL_TYPE");
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

    public AllType(Table t) {
        super(t, "ALL_TYPE");
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
    protected AllType newInstance(Table t) {
        return new AllType(t);
    }

    static class AllTypeDeserializer extends QueryDeserializer<AllType> {
        @Override
        protected Query<AllType> newQueryInstance() {
            return new AllType();
        }
    }
}
