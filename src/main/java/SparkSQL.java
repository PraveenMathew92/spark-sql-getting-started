import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

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
        Dataset<Row> dataframe = sparkSession.read()
                .json(JSON_FILE_PATH);

        // Display the Dataframe with the contents
        dataframe.show();

        // Show only one column of the dataframe
        dataframe.select("histogram").show();

        // Print the json schema of the dataframe
        dataframe.printSchema();

        // Use SQL on Dataframe
        dataframe.createOrReplaceTempView("dataframeView");
        sparkSession.sql("SELECT speed FROM dataframeView")
                    .show();

        // Explode a column of array to individual elements
        dataframe.select(explode(col("histogram")))
                .show();
    }
}
