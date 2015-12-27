package com.kingcobra.weatherws;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.*;

/**
 * Created by kingcobra on 15/11/28.
 * 此类开启spring注解模式,等同于在dispatcherServlet中设置<mvc:annotation-driven/>
 */
@Configuration
@EnableWebMvc
public class WebConfig extends WebMvcConfigurerAdapter {
    /**
     * 设置默认的servlet,等同于设置<mvc:default-servlet-handler/>
     * @param configurer
     */
    @Override
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        //指定默认的servlet名称
        configurer.enable("weather-ws");
    }

    @Override
    public void configureViewResolvers(ViewResolverRegistry registry) {
        registry.jsp("/", ".jsp");
    }
}
