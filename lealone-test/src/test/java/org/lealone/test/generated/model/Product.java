package org.lealone.test.generated.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.lealone.orm.Model;
import org.lealone.orm.ModelDeserializer;
import org.lealone.orm.ModelProperty;
import org.lealone.orm.ModelSerializer;
import org.lealone.orm.ModelTable;
import org.lealone.orm.property.PDouble;
import org.lealone.orm.property.PLong;
import org.lealone.orm.property.PString;
import org.lealone.test.generated.model.Product.ProductDeserializer;

/**
 * Model for table 'PRODUCT'.
 *
 * THIS IS A GENERATED OBJECT, DO NOT MODIFY THIS CLASS.
 */
@JsonSerialize(using = ModelSerializer.class)
@JsonDeserialize(using = ProductDeserializer.class)
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
        super.setRoot(this);

        this.productId = new PLong<>("PRODUCT_ID", this);
        this.productName = new PString<>("PRODUCT_NAME", this);
        this.category = new PString<>("CATEGORY", this);
        this.unitPrice = new PDouble<>("UNIT_PRICE", this);
        super.setModelProperties(new ModelProperty[] { this.productId, this.productName, this.category, this.unitPrice });
    }

    @Override
    protected Product newInstance(ModelTable t, short modelType) {
        return new Product(t, modelType);
    }

    static class ProductDeserializer extends ModelDeserializer<Product> {
        @Override
        protected Model<Product> newModelInstance() {
            return new Product();
        }
    }
}
