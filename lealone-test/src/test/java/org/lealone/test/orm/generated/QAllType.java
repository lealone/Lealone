package org.lealone.test.orm.generated;

import org.lealone.orm.Query;
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

/**
 * Query bean for model 'AllType'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class QAllType extends Query<AllType, QAllType> {

    public static QAllType create(String url) {
        Table t = new Table(url, "ALL_TYPE");
        return new QAllType(t);
    }

    public final PInteger<QAllType> f1;
    public final PBoolean<QAllType> f2;
    public final PByte<QAllType> f3;
    public final PShort<QAllType> f4;
    public final PLong<QAllType> f5;
    public final PLong<QAllType> f6;
    public final PBigDecimal<QAllType> f7;
    public final PDouble<QAllType> f8;
    public final PFloat<QAllType> f9;
    public final PTime<QAllType> f10;
    public final PDate<QAllType> f11;
    public final PTimestamp<QAllType> f12;
    public final PBytes<QAllType> f13;
    public final PObject<QAllType> f14;
    public final PString<QAllType> f15;
    public final PString<QAllType> f16;
    public final PString<QAllType> f17;
    public final PBlob<QAllType> f18;
    public final PClob<QAllType> f19;
    public final PUuid<QAllType> f20;
    public final PArray<QAllType> f21;

    private QAllType(Table t) {
        super(t);
        setRoot(this);

        this.f1 = new PInteger<>("f1", this);
        this.f2 = new PBoolean<>("f2", this);
        this.f3 = new PByte<>("f3", this);
        this.f4 = new PShort<>("f4", this);
        this.f5 = new PLong<>("f5", this);
        this.f6 = new PLong<>("f6", this);
        this.f7 = new PBigDecimal<>("f7", this);
        this.f8 = new PDouble<>("f8", this);
        this.f9 = new PFloat<>("f9", this);
        this.f10 = new PTime<>("f10", this);
        this.f11 = new PDate<>("f11", this);
        this.f12 = new PTimestamp<>("f12", this);
        this.f13 = new PBytes<>("f13", this);
        this.f14 = new PObject<>("f14", this);
        this.f15 = new PString<>("f15", this);
        this.f16 = new PString<>("f16", this);
        this.f17 = new PString<>("f17", this);
        this.f18 = new PBlob<>("f18", this);
        this.f19 = new PClob<>("f19", this);
        this.f20 = new PUuid<>("f20", this);
        this.f21 = new PArray<>("f21", this);
    }
}
