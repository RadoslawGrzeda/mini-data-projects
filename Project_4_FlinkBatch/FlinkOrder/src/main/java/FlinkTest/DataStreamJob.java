
package FlinkTest;
import FlinkTest.entities.OrderDetail;
import FlinkTest.entities.Orders;
import FlinkTest.entities.Restaurants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
//import org.apache.flink.streaming.api.functions.co.JoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Date;
import java.util.Timer;

import static java.sql.Time.*;

@Slf4j
public class DataStreamJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.BATCH);

//		String RestaurantsPath = "Datasets/Restaurants.csv";
		String RestaurantsPath = "/mnt/c/Users/Radosław/Desktop/mini-data-projects/Project_4_FlinkBatch/FlinkOrder/Datasets/Restaurants.csv";
		String OrdersPath      = "/mnt/c/Users/Radosław/Desktop/mini-data-projects/Project_4_FlinkBatch/FlinkOrder/Datasets/Orders.csv";

//		String RestaurantsPath="C:\\Users\\Radosław\\Desktop\\mini-data-projects\\Project_4_FlinkBatch\\FlinkOrder\\Datasets\\Restaurants.csv";
		readCSV mp = new readCSV(env,RestaurantsPath,"restaurants","RestaurantID");

		DataStream<Restaurants> restaurants=mp.dtStream.map(line-> {
			String [] arr =  line.split(",");
			Integer RestaurantID=Integer.parseInt(arr[0]);
			String RestaurantName= arr[1];
			String Cuisine=arr[2];
			String Zone=arr[3];
			String Category=arr[4];
			return new Restaurants(RestaurantID,RestaurantName,Cuisine,Zone,Category);
		}).returns(Restaurants.class);

//		restaurants.print();

//		String OrdersPath="C:\\Users\\Radosław\\Desktop\\mini-data-projects\\Project_4_FlinkBatch\\FlinkOrder\\Datasets\\Orders.csv";
//		String OrdersPath="Da";
//		String OrdersPath      = "Datasets/Orders.csv";

		readCSV ordCSV =  new readCSV(env,OrdersPath,"orders","Order ID");

		DataStream<Orders> Orders = ordCSV.dtStream.map(line -> {
			String[] arr = line.split(",");
			String Order_Id = arr[0];
			String Customer_Id=arr[1];
			Integer Resturant_Id=Integer.parseInt(arr[2]);
			SimpleDateFormat sdf = new SimpleDateFormat("d/m/yyyy HH:mm");
			Date Order_Date=sdf.parse(arr[3]);
			int Quantity_of_Items=Integer.parseInt(arr[4]);
			int Order_Amount=Integer.parseInt(arr[5]);
			String Payment_Mode=arr[6];
			int Delivery_Time=Integer.parseInt(arr[7]);
			int Customer_Rating_Food=Integer.parseInt(arr[8]);
			int Customer_Rating_Delivery=Integer.parseInt(arr[9]);

			return new Orders( Order_Id,  Customer_Id,Resturant_Id,Order_Date,Quantity_of_Items,Order_Amount,Payment_Mode,Delivery_Time,Customer_Rating_Food,Customer_Rating_Delivery);
		}).returns(Orders.class);



		DataStream<OrderDetail> OrderDetail = Orders
				.join(restaurants).where(FlinkTest.entities.Orders::getKey)
				.equalTo(Restaurants::getKey)
				.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
				.apply(new JoinFunction<Orders, Restaurants, OrderDetail>() {
						@Override
						public OrderDetail join(Orders o, Restaurants r) throws Exception {
						Integer overallRating=o.getCustomer_Rating_Delivery()+o.getCustomer_Rating_Food()/2;
						return new OrderDetail(
								r.getRestaurantName(),
								r.getCuisine(),
								r.getCategory(),
								o.getCustomer_Id(),
								o.getOrder_Id(),
								o.getOrder_Date(),
								overallRating,
								o.getOrder_Amount()
								);
								}});
//		OrderDetail.print();




/*


		FileSource<String> source2 = FileSource
					.forRecordStreamFormat(new TextLineInputFormat(),new Path("C:\\Users\\Radosław\\Desktop\\mini-data-projects\\Project_4_FlinkBatch\\FlinkOrder\\Datasets\\Orders.csv"))
					.build();

		DataStream<String> lines = env.fromSource(
				source2,
				WatermarkStrategy.noWatermarks(),
				"Orders"
		);

		var lines2=lines.filter(line -> !line.startsWith("Order ID"));

		lines2.print();

		DataStream<Orders> orders = lines.map(record -> {
			String[] arr = record.split(",");

			String Order_Id= String.valueOf((arr[0]));
			String Customer_Id= arr[1];
			String Resturant_Id = arr[2];
//			DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			DateFormat dateFormat = new SimpleDateFormat("d/m/yyyy HH:mm");
			Date date = dateFormat.parse(arr[3]);
			Instant instant = date.toInstant();
			Date Order_Date=Date.from(instant);
			int Quantity_of_Items= Integer.parseInt(arr[4]);
			int Order_Amount = Integer.parseInt(arr[5]);
			String Payment_Mode = arr[6];
			int Delivery_Time = Integer.parseInt(arr[7]);
			int Customer_Rating_Food= Integer.parseInt(arr[8]);
			int Customer_Rating_Delivery= Integer.parseInt(arr[9]);

return new Orders( Order_Id,  Customer_Id,Resturant_Id,Order_Date,Quantity_of_Items,Order_Amount,Payment_Mode,Delivery_Time,Customer_Rating_Food,Customer_Rating_Delivery);
		}).returns(Orders.class);
		orders.print();
*/







//		orders.print();
////		DataSet<String> orders = env.readFile("Datasets/Orders.csv");
//		FileSource<String> source2 = FileSource
//				.forRecordStreamFormat(new TextLineInputFormat(), new Path("Datasets/Orders.csv"))
//				.build();
		env.execute("Flink Java API Skeleton");
	}
}
