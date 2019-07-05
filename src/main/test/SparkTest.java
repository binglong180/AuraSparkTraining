import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class SparkTest {
    static SparkSession spark = null;
    static{
        spark = SparkSession.builder().master("local").appName("sparkTets").getOrCreate();
    }

    public static void main(String[] args){
        test1();
    }

    public static void test1(){
        Dataset<Row> json = spark.read().json("data/ml-1m/users.json");

        json.printSchema();
        json.show(4);
        json.limit(2).toJSON().foreach(x -> System.out.println("limit:"+x));

        json.withColumn("age2",col("age").plus(1));
        System.out.println("collect:------"+json.collect());


        json.select("userID", "age").limit(2).show();

        json.selectExpr("userID","ceil(age/10) as newAge ").show(2);

        json.select(max("age"),min("age"),avg("age")).show();


        json.select("age").show();
        json.withColumn("age2",col("age").plus(1));
        json.filter(col("age").gt(30)).show();

        spark.stop();
    }

    public static void test2(){

    }
}
