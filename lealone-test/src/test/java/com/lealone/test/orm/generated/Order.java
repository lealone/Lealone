package com.lealone.test.orm.generated;

import com.lealone.orm.Model;
import com.lealone.orm.ModelProperty;
import com.lealone.orm.ModelTable;
import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.property.PDate;
import com.lealone.orm.property.PDouble;
import com.lealone.orm.property.PInteger;
import com.lealone.orm.property.PLong;
import java.util.List;

/**
 * Model for table 'ORDER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
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
        customerId = new PLong<>("CUSTOMER_ID", this);
        orderId = new PInteger<>("ORDER_ID", this);
        orderDate = new PDate<>("ORDER_DATE", this);
        total = new PDouble<>("TOTAL", this);
        super.setModelProperties(new ModelProperty[] { customerId, orderId, orderDate, total });
        super.initSetters(new CustomerSetter());
        super.initAdders(new OrderItemAdder());
    }

    @Override
    protected Order newInstance(ModelTable t, short modelType) {
        return new Order(t, modelType);
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
        super.addModel(m);
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

    protected class CustomerSetter implements AssociateSetter<Customer> {
        @Override
        public Customer getDao() {
            return Customer.dao;
        }

        @Override
        public boolean set(Customer m) {
            if (areEqual(customerId, m.id)) {
                setCustomer(m);
                return true;
            }
            return false;
        }
    }

    protected class OrderItemAdder implements AssociateAdder<OrderItem> {
        @Override
        public OrderItem getDao() {
            return OrderItem.dao;
        }

        @Override
        public void add(OrderItem m) {
            if (areEqual(orderId, m.orderId)) {
                addOrderItem(m);
            }
        }
    }

    public static Order decode(String str) {
        return decode(str, null);
    }

    public static Order decode(String str, JsonFormat format) {
        return new Order().decode0(str, format);
    }
}
