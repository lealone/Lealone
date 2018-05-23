package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.ArrayList;
import java.util.List;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PDate;
import org.lealone.orm.property.PDouble;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.test.generated.model.Order.OrderDeserializer;

/**
 * Model for table 'ORDER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = OrderDeserializer.class)
public class Order extends Model<Order> {

    public static final Order dao = new Order(null, ROOT_DAO);

    public final PLong<Order> customerId;
    public final PInteger<Order> orderId;
    public final PDate<Order> orderDate;
    public final PDouble<Order> total;
    private Customer customer;

    public Order() {
        this(null, REGULAR_MODEL);
    }

    private Order(ModelTable t, short modelType) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "ORDER") : t, modelType);
        super.setRoot(this);

        this.customerId = new PLong<>("CUSTOMER_ID", this);
        this.orderId = new PInteger<>("ORDER_ID", this);
        this.orderDate = new PDate<>("ORDER_DATE", this);
        this.total = new PDouble<>("TOTAL", this);
        super.setModelProperties(new ModelProperty[] { this.customerId, this.orderId, this.orderDate, this.total });
    }

    public Customer getCustomer() {
        return customer;
    }

    public Order setCustomer(Customer customer) {
        this.customer = customer;
        this.customerId.set(customer.id.get());
        return this;
    }

    public Order addOrderItem(OrderItem m) {
        m.setOrder(this);
        super.addModel(m);;
        return this;
    }

    public Order addOrderItem(OrderItem... mArray) {
        for (OrderItem m : mArray)
            addOrderItem(m);
        return this;
    }

    public List<OrderItem> getOrderItemList() {
        return super.getModelList(OrderItem.class);
    }

    @Override
    protected Order newInstance(ModelTable t, short modelType) {
        return new Order(t, modelType);
    }

    @Override
    protected List<Model<?>> newAssociateInstances() {
        ArrayList<Model<?>> list = new ArrayList<>();
        OrderItem m1 = new OrderItem();
        addOrderItem(m1);
        list.add(m1);
        return list;
    }

    static class OrderDeserializer extends ModelDeserializer<Order> {
        @Override
        protected Model<Order> newModelInstance() {
            return new Order();
        }
    }
}
