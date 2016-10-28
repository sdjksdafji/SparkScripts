import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by sdjksdafji on 10/27/16.
 */
public class ReadJson {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Read Json from Google Cloud Storage")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("examples/src/main/resources/people.json");

        // Displays the content of the DataFrame to stdout
        df.printSchema();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT COUNT(*) FROM people");
        sqlDF.show();
    }
}
