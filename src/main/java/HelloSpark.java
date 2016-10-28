/**
 * Created by sdjksdafji on 10/27/16.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;


// spark-submit --class HelloSpark --master "spark://Shuyi-MBP-Ubuntu:7077" HelloSpark-1.0-SNAPSHOT.jar

public class HelloSpark {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Estimate PI Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        int NUM_SAMPLES = 3000000;
        List<Integer> l = new ArrayList<Integer>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l).filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer i) {
                double x = Math.random();
                double y = Math.random();
                return x*x + y*y < 1;
            }
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}