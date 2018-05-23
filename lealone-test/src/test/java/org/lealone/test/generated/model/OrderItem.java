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
import org.lealone.test.generated.model.OrderItem.OrderItemDeserializer;

/**
 * Model for table 'ORDER_ITEM'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = OrderItemDeserializer.class)
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
        super.setRoot(this);

        this.orderId = new PInteger<>("ORDER_ID", this);
        this.productId = new PLong<>("PRODUCT_ID", this);
        this.productCount = new PInteger<>("PRODUCT_COUNT", this);
        super.setModelProperties(new ModelProperty[] { this.orderId, this.productId, this.productCount });
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

    static class OrderItemDeserializer extends ModelDeserializer<OrderItem> {
        @Override
        protected Model<OrderItem> newModelInstance() {
            return new OrderItem();
        }
    }
}
