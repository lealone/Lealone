package com.lealone.test.orm.generated;

import com.lealone.orm.Model;
import com.lealone.orm.ModelProperty;
import com.lealone.orm.ModelTable;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.property.PDouble;
import com.lealone.orm.property.PLong;
import com.lealone.orm.property.PString;
import java.util.List;

/**
 * Model for table 'PRODUCT'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class Product extends Model<Product> {

    public static final Product dao = new Product(null, ROOT_DAO);

    public final PLong<Product> productId;
    public final PString<Product> productName;
    public final PString<Product> category;
    public final PDouble<Product> unitPrice;

    public Product() {
        this(null, REGULAR_MODEL);
    }

    private Product(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "PRODUCT") : t, modelType);
        productId = new PLong<>("PRODUCT_ID", this);
        productName = new PString<>("PRODUCT_NAME", this);
        category = new PString<>("CATEGORY", this);
        unitPrice = new PDouble<>("UNIT_PRICE", this);
        super.setModelProperties(new ModelProperty[] { productId, productName, category, unitPrice });
        super.initAdders(new OrderItemAdder());
    }

    @Override
    protected Product newInstance(ModelTable t, short modelType) {
        return new Product(t, modelType);
    }

    public Product addOrderItem(OrderItem m) {
        m.setProduct(this);
        super.addModel(m);
        return this;
    }

    public Product addOrderItem(OrderItem... mArray) {
        for (OrderItem m : mArray)
            addOrderItem(m);
        return this;
    }

    public List<OrderItem> getOrderItemList() {
        return super.getModelList(OrderItem.class);
    }

    protected class OrderItemAdder implements AssociateAdder<OrderItem> {
        @Override
        public OrderItem getDao() {
            return OrderItem.dao;
        }

        @Override
        public void add(OrderItem m) {
            if (areEqual(productId, m.productId)) {
                addOrderItem(m);
            }
        }
    }

    public static Product decode(Object obj) {
        return decode(obj, null);
    }

    public static Product decode(Object obj, JsonFormat format) {
        return new Product().decode0(obj, format);
    }
}
