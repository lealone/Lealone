package org.lealone.test.orm.generated;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.lealone.orm.Table;

/**
 * Model bean for table 'ALL_TYPE'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class AllType {

    public static AllType create(String url) {
        Table t = new Table(url, "ALL_TYPE");
        return new AllType(t);
    }

    private Table _t_;

    private Integer f1;
    private Boolean f2;
    private Byte f3;
    private Short f4;
    private Long f5;
    private Long f6;
    private BigDecimal f7;
    private Double f8;
    private Float f9;
    private Time f10;
    private Date f11;
    private Timestamp f12;
    private byte[] f13;
    private Object f14;
    private String f15;
    private String f16;
    private String f17;
    private Blob f18;
    private Clob f19;
    private UUID f20;
    private Array f21;

    public AllType() {
    }

    private AllType(Table t) {
        this._t_ = t;
    }

    public AllType setF1(Integer f1) {
        this.f1 = f1; 
        return this;
    }

    public Integer getF1() { 
        return f1; 
    }

    public AllType setF2(Boolean f2) {
        this.f2 = f2; 
        return this;
    }

    public Boolean getF2() { 
        return f2; 
    }

    public AllType setF3(Byte f3) {
        this.f3 = f3; 
        return this;
    }

    public Byte getF3() { 
        return f3; 
    }

    public AllType setF4(Short f4) {
        this.f4 = f4; 
        return this;
    }

    public Short getF4() { 
        return f4; 
    }

    public AllType setF5(Long f5) {
        this.f5 = f5; 
        return this;
    }

    public Long getF5() { 
        return f5; 
    }

    public AllType setF6(Long f6) {
        this.f6 = f6; 
        return this;
    }

    public Long getF6() { 
        return f6; 
    }

    public AllType setF7(BigDecimal f7) {
        this.f7 = f7; 
        return this;
    }

    public BigDecimal getF7() { 
        return f7; 
    }

    public AllType setF8(Double f8) {
        this.f8 = f8; 
        return this;
    }

    public Double getF8() { 
        return f8; 
    }

    public AllType setF9(Float f9) {
        this.f9 = f9; 
        return this;
    }

    public Float getF9() { 
        return f9; 
    }

    public AllType setF10(Time f10) {
        this.f10 = f10; 
        return this;
    }

    public Time getF10() { 
        return f10; 
    }

    public AllType setF11(Date f11) {
        this.f11 = f11; 
        return this;
    }

    public Date getF11() { 
        return f11; 
    }

    public AllType setF12(Timestamp f12) {
        this.f12 = f12; 
        return this;
    }

    public Timestamp getF12() { 
        return f12; 
    }

    public AllType setF13(byte[] f13) {
        this.f13 = f13; 
        return this;
    }

    public byte[] getF13() { 
        return f13; 
    }

    public AllType setF14(Object f14) {
        this.f14 = f14; 
        return this;
    }

    public Object getF14() { 
        return f14; 
    }

    public AllType setF15(String f15) {
        this.f15 = f15; 
        return this;
    }

    public String getF15() { 
        return f15; 
    }

    public AllType setF16(String f16) {
        this.f16 = f16; 
        return this;
    }

    public String getF16() { 
        return f16; 
    }

    public AllType setF17(String f17) {
        this.f17 = f17; 
        return this;
    }

    public String getF17() { 
        return f17; 
    }

    public AllType setF18(Blob f18) {
        this.f18 = f18; 
        return this;
    }

    public Blob getF18() { 
        return f18; 
    }

    public AllType setF19(Clob f19) {
        this.f19 = f19; 
        return this;
    }

    public Clob getF19() { 
        return f19; 
    }

    public AllType setF20(UUID f20) {
        this.f20 = f20; 
        return this;
    }

    public UUID getF20() { 
        return f20; 
    }

    public AllType setF21(Array f21) {
        this.f21 = f21; 
        return this;
    }

    public Array getF21() { 
        return f21; 
    }

    public void save() {
        _t_.save(this);
    }

    public boolean delete() {
       return _t_.delete(this);
    }
}
