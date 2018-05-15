package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PDate;
import org.lealone.orm.property.PDouble;
import org.lealone.orm.property.PInteger;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.TQProperty;
import org.lealone.test.generated.model.Order.OrderDeserializer;

/**
 * Model for table 'ORDER'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = OrderDeserializer.class)
public class Order extends Model<Order> {

    public static final Order dao = new Order(null, true);

    public static Order create(String url) {
        ModelTable t = new ModelTable(url, "TEST", "PUBLIC", "ORDER");
        return new Order(t);
    }

    public final PLong<Order> customerId;
    public final PInteger<Order> orderId;
    public final PDate<Order> orderDate;
    public final PDouble<Order> total;

    public Order() {
        this(null);
    }

    private Order(ModelTable t) {
        this(t, false);
    }

    private Order(ModelTable t, boolean isDao) {
        super(t == null ? new ModelTable("TEST", "PUBLIC", "ORDER") : t, isDao);
        super.setRoot(this);

        this.customerId = new PLong<>("CUSTOMERID", this);
        this.orderId = new PInteger<>("ORDERID", this);
        this.orderDate = new PDate<>("ORDERDATE", this);
        this.total = new PDouble<>("TOTAL", this);
        super.setTQProperties(new TQProperty[] { this.customerId, this.orderId, this.orderDate, this.total });
    }

    @Override
    protected Order newInstance(ModelTable t) {
        return new Order(t);
    }

    static class OrderDeserializer extends ModelDeserializer<Order> {
        @Override
        protected Model<Order> newModelInstance() {
            return new Order();
        }
    }
}
