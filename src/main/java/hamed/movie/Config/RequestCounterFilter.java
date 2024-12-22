package hamed.movie.Config;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class RequestCounterFilter implements Filter {
    private int requestCount = 0;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        requestCount++;
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        System.out.println("Request Count: " + requestCount);
        chain.doFilter(request, response);
    }

    public int getRequestCount() {
        return requestCount;
    }
}
