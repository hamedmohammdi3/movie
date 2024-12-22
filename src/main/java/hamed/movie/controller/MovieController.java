package hamed.movie.controller;

import hamed.movie.Config.RequestCounterFilter;
import hamed.movie.service.BasicService;
import org.apache.spark.sql.Row;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class MovieController  {
    private final BasicService basicService;
    private final RequestCounterFilter requestCounterFilter;

    public MovieController(BasicService basicService, RequestCounterFilter requestCounterFilter) {
        this.basicService = basicService;
        this.requestCounterFilter = requestCounterFilter;
    }

    @GetMapping("/request-count")
    public int getRequestCount() {
        return requestCounterFilter.getRequestCount();
    }

    @GetMapping("/same-director-writer")
    public List<Row> getTitlesWithSameDirectorAndWriter() {
       return basicService.loadCrewBasicsAndNameData();
    }

    @GetMapping("/titles-with-actors")
    public List<Row> getTitlesWithActors(@RequestParam String actor1, @RequestParam String actor2) {
        return basicService.loadPrincipalsAndBasicsData(actor1, actor2);
    }

    @GetMapping("/best-titles-by-genre")
    public List<Row> getBestTitlesByGenre(@RequestParam String genre) {
        return basicService.loadRatingsAndBasicsData(genre);
    }


}