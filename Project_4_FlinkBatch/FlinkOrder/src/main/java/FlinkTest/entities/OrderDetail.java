package FlinkTest.entities;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Date;

@Data
@AllArgsConstructor
public class OrderDetail {
    private String Restaurant_Name;
    private String Cuisine;
    private String Category;
    private String CustomerId;
    private String Order_Id;
    private Date OrderDate;
    private Integer overallRating;
    private double Order_amount;
}
