package org.lealone.test.orm.generated;

import org.lealone.orm.Model;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;

/**
 * Model for table 'ORDER_ITEM'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
public class OrderItem extends Model<OrderItem> {

    public static final OrderItem dao = new OrderItem(null, ROOT_DAO);

    public final PInteger<OrderItem> orderId;
    public final PLong<OrderItem> productId;
    public final PInteger<OrderItem> productCount;
    private Order order;
    private Product product;

    public OrderItem() {
        this(null, REGULAR_MODEL);
    }

    private OrderItem(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "ORDER_ITEM") : t, modelType);
        orderId = new PInteger<>("ORDER_ID", this);
        productId = new PLong<>("PRODUCT_ID", this);
        productCount = new PInteger<>("PRODUCT_COUNT", this);
        super.setModelProperties(new ModelProperty[] { orderId, productId, productCount });
    }

    public Order getOrder() {
        return order;
    }

    public OrderItem setOrder(Order order) {
        this.order = order;
        this.orderId.set(order.orderId.get());
        return this;
    }

    public Product getProduct() {
        return product;
    }

    public OrderItem setProduct(Product product) {
        this.product = product;
        this.productId.set(product.productId.get());
        return this;
    }

    @Override
    protected OrderItem newInstance(ModelTable t, short modelType) {
        return new OrderItem(t, modelType);
    }

    public static OrderItem decode(String str) {
        return new OrderItem().decode0(str);
    }
}
