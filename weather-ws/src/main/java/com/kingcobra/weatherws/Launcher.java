package com.kingcobra.weatherws;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.config.PropertySetter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.*;
import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.util.Log4jConfigListener;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kingcobra on 15/11/28.
 */
public class Launcher {

    public void execute(String ip, int port){

        final Server server = new Server();
        //创建server连接器，经过以下配置可以在浏览器中使用http://ip:port访问jetty
        final ServerConnector serverConnector = new ServerConnector(server);
        serverConnector.setPort(port);
        serverConnector.setReuseAddress(true);
        serverConnector.setIdleTimeout(60000);
        serverConnector.setHost(ip);
        server.addConnector(serverConnector);

        //创建servletContext处理器
        final ServletContextHandler contextHandler = new ServletContextHandler();

        //创建springWebApp实例
        final WebAppInitializer webAppInitializer = new WebAppInitializer();

        //创建支持注解的WebApplicationContext
        final AnnotationConfigWebApplicationContext annotationConfigWebApplicationContext = (AnnotationConfigWebApplicationContext)webAppInitializer.createServletApplicationContext();
        //指定注解模式扫描的包
        annotationConfigWebApplicationContext.scan("com.kingcobra");

        //初始化SpringMVC的dispatcherServlet,
        final DispatcherServlet dispatcherServlet = new DispatcherServlet();
        dispatcherServlet.setApplicationContext(annotationConfigWebApplicationContext);

        //以下相当于配置web.xml
        final ServletHandler servletHandler = new ServletHandler();
        servletHandler.addServletWithMapping(new ServletHolder(dispatcherServlet), "/");
        ListenerHolder listenerHolder = new ListenerHolder(BaseHolder.Source.DESCRIPTOR);
        listenerHolder.setListener(new ContextLoaderListener());
        servletHandler.addListener(listenerHolder);
        contextHandler.setServletHandler(servletHandler);

        //创建Handler集合类
        HandlerCollection handlers = new HandlerCollection();
        handlers.addHandler(contextHandler);
        server.setHandler(handlers);

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("please input ip and port!");
            System.exit(-1);
        }
        Pattern pattern = Pattern.compile("(\\d{0,3}).(\\d{0,3}).(\\d{0,3}).(\\d{0,3})");
        Matcher matcher = pattern.matcher(args[0]);
        if(!matcher.matches()){
            System.out.println("the first param is ip,please check ip param!");
            System.exit(-1);
        }
        Pattern pattern1 = Pattern.compile("\\d{2,5}");
        Matcher matcher1 = pattern1.matcher(args[1]);
        if(!matcher1.matches()){
            System.out.println("the second param is port,please check param!");
            System.exit(-1);
        }
        System.setProperty("port", args[1]);
        Launcher launcher = new Launcher();
        launcher.execute(args[0], Integer.valueOf(args[1]));
    }

}
