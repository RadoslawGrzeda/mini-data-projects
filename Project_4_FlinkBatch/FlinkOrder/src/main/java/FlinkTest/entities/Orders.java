package FlinkTest.entities;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.functions.KeySelector;

import java.util.Date;

@Data
@NoArgsConstructor
public class Orders  {
    private String Order_Id;
    private String Customer_Id;
    private Integer Resturant_Id;
    private Date Order_Date;
    private int Quantity_of_Items;
    private int Order_Amount;
    private String Payment_Mode;
    private int Delivery_Time;
    private int Customer_Rating_Food;
    private int Customer_Rating_Delivery;


    public Orders(String order_Id, String customer_Id, Integer resturant_Id, Date order_Date, int quantity_of_Items, int order_Amount, String payment_Mode, int delivery_Time, int customer_Rating_Food, int customer_Rating_Delivery) {
        Order_Id = order_Id;
        Customer_Id = customer_Id;
        Resturant_Id = resturant_Id;
        Order_Date = order_Date;
        Quantity_of_Items = quantity_of_Items;
        Order_Amount = order_Amount;
        Payment_Mode = payment_Mode;
        Delivery_Time = delivery_Time;
        Customer_Rating_Food = customer_Rating_Food;
        Customer_Rating_Delivery = customer_Rating_Delivery;
    }
    public Integer getKey(){
        return Resturant_Id;
    }
}
//    @Override
//    public Object getKey(Object o) throws Exception {
//        return Resturant_Id;
//    }

