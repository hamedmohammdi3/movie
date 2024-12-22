package hamed.movie.service;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Service
public class BasicService {
    @Autowired
    private SparkSession sparkSession;
    private Dataset<Row> crewDf;
    private Dataset<Row> nameDf;
    private Dataset<Row> principalsDf;
    private Dataset<Row> basicsDf;
    private Dataset<Row> ratingsDf;

    @PostConstruct
    public void init() {
        String filePathCrew = "title.crew.tsv";
        crewDf = sparkSession.read().option("header", "true").option("delimiter","\t").csv(filePathCrew);
        String filePathName = "name.basics.tsv";
        nameDf = sparkSession.read().option("header", "true").option("delimiter","\t").csv(filePathName);
        String filePathPrincipals = "title.principals.tsv";
        principalsDf = sparkSession.read().option("header", "true").option("delimiter","\t").csv(filePathPrincipals);
        String filePathBasics = "title.basics.tsv";
        basicsDf = sparkSession.read().option("header", "true").option("delimiter","\t").csv(filePathBasics);
        String filePathRatings = "title.ratings.tsv";
        ratingsDf = sparkSession.read().option("header", "true").option("delimiter","\t").csv(filePathRatings);
    }



    public List<Row> loadCrewBasicsAndNameData() {
        return crewDf
                .join(nameDf, crewDf.col("directors").equalTo(nameDf.col("nconst"))
                        .or(crewDf.col("writers").equalTo(nameDf.col("nconst")))
                        .and(nameDf.col("deathYear").isNull()), "inner")
                .select("tconst", "primaryName")
                .collectAsList();
    }

    public List<Row> loadPrincipalsAndBasicsData(String actor1, String actor2) {
        Dataset<Row> actor1Titles = principalsDf.filter(principalsDf.col("nconst").equalTo(actor1)).select("tconst");
        Dataset<Row> actor2Titles = principalsDf.filter(principalsDf.col("nconst").equalTo(actor2)).select("tconst");

        return actor1Titles.intersect(actor2Titles).join(basicsDf, "tconst").collectAsList();
    }

    public List<Row> loadRatingsAndBasicsData(String genre) {
        return basicsDf.filter(basicsDf.col("genres").contains(genre))
                .join(ratingsDf, "tconst")
                .groupBy("startYear")
                .agg(
                        org.apache.spark.sql.functions.max("averageRating").alias("maxRating"),
                        org.apache.spark.sql.functions.first("tconst").alias("bestTitle")
                ).collectAsList();
    }
}
