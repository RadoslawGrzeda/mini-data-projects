package FlinkTest.entities;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Restaurants {
    private Integer RestaurantID;
    private String RestaurantName;
    private String Cuisine;
    private String Zone;
    private String Category;

    public Restaurants(Integer restaurantID, String restaurantName, String cuisine, String zone, String category) {
        RestaurantID = restaurantID;
        RestaurantName = restaurantName;
        Cuisine = cuisine;
        Zone = zone;
        Category = category;
    }
    public Integer getKey(){
        return RestaurantID;
    }
}
