import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.columnar.LONG;
import org.apache.spark.sql.types.DataTypes;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Created by Shuyi Wang on 10/27/16.
 */
public class ReadJson {
    @Option(name="-dataset", usage="Location of data set. Should be in JSON format")
    private String dataset = "gs://friend_recommendation/06172016/friend_seen_training_filtering/part*";

    public static void main(String[] args) {
        new ReadJson().doMain(args);
    }

    private void doMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("Read Json from Google Cloud Storage")
                .getOrCreate();

        Dataset<Row> df = spark.read().json(dataset);

        df.withColumn("ageDifference", df.col("ageDifferenceF").cast(DataTypes.IntegerType))
                .withColumn("mutualFriend", df.col("mutualFriendF").cast(DataTypes.IntegerType))
                .withColumn("score", df.col("scoreF").cast(DataTypes.DoubleType))
                .withColumn("userActivity", df.col("userActivityF").cast(DataTypes.IntegerType))
                .withColumn("friendActivity", df.col("friendActivityF").cast(DataTypes.IntegerType))
                .withColumn("userFriendCount", df.col("userFriendCountF").cast(DataTypes.IntegerType))
                .withColumn("friendFriendCount", df.col("friendFriendCountF").cast(DataTypes.IntegerType));

        // Displays the content of the DataFrame to stdout
        df.printSchema();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT COUNT(*) FROM people");
        sqlDF.show();
        Dataset<Row> lineDF = spark.sql("SELECT * FROM people LIMIT 100");
        lineDF.show();
    }
}
