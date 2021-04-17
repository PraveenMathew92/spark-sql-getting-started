import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
    private static final String JSON_FILE_PATH =
            "/Users/praveenmathew/Work/side_projects/Spark/typing-data-spark-sql/src/main/resources/typing-data.json";


    public static void main(String[] args) {

        // Create a Spark Session in the local machine
        SparkSession sparkSession = SparkSession.builder()
                .appName("Typing Data")
                .master("local")
                .getOrCreate();

        // Read the python data set
        Dataset<Row> json = sparkSession.read()
                .json(JSON_FILE_PATH);

        json.show();
        json.printSchema();
    }
}
