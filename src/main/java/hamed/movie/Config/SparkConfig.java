package hamed.movie.Config;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName("Spring Boot with Spark")
                .master("local[*]")
                .config("spark.executor.memory", "8g")
                .config("spark.executor.cores", "7")
                .config("spark.driver.memory", "8g")
                .getOrCreate();
    }
}  