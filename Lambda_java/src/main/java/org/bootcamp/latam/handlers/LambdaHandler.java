package org.bootcamp.latam.handlers;

import com.amazonaws.serverless.exceptions.ContainerInitializationException;
import com.amazonaws.serverless.proxy.InitializationWrapper;
import com.amazonaws.serverless.proxy.model.AwsProxyRequest;
import com.amazonaws.serverless.proxy.model.AwsProxyResponse;
import com.amazonaws.serverless.proxy.spring.SpringBootLambdaContainerHandler;
import com.amazonaws.serverless.proxy.spring.SpringBootProxyHandlerBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.bootcamp.latam.main.MainApplication;

import org.bootcamp.latam.model.LambdaResponse;
import org.bootcamp.latam.model.Param;
import org.bootcamp.latam.model.Publication;
import org.bootcamp.latam.model.Question;
import org.bootcamp.latam.service.AthenaServiceImpl;
import org.bootcamp.latam.service.IAthenaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class LambdaHandler  implements RequestHandler<Param,LambdaResponse> {

    private static SpringBootLambdaContainerHandler<AwsProxyRequest, AwsProxyResponse> handler;
    private static final Logger logger = LoggerFactory.getLogger(LambdaHandler.class);
    private IAthenaService athenaService= new AthenaServiceImpl<>();

    static {
        try {
            handler = new SpringBootProxyHandlerBuilder<AwsProxyRequest>()
                    .defaultProxy()
                    .initializationWrapper(new InitializationWrapper())
                    .servletApplication()
                    .springBootApplication(MainApplication.class)
                    .buildAndInitialize();
        } catch (ContainerInitializationException e) {
            e.printStackTrace();
            throw new RuntimeException("Could not initialize Spring Boot application", e);
        }
    }




    private static InputStream clone(final InputStream inputStream) {
        logger.info("Cloning InputStream");
        try {
            inputStream.mark(0);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int readLength = 0;
            while ((readLength = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, readLength);
            }
            inputStream.reset();
            outputStream.flush();
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    @Override
    public LambdaResponse handleRequest(Param input, Context context) {
        logger.info("Input received: "+ input.toString()+ " "+ context.toString());
        List<Publication> publicationList=athenaService.getDataFromAthena("Select * from workshoplabkc.publication limit 10");
        ResponseManager responseManager = new ResponseManager();
        for (Question question: input.getQuestions()) {
            responseManager.add(question.getQuestion(),publicationList,publicationList.size());
        }

        LambdaResponse lambdaResponse= new LambdaResponse();
        lambdaResponse.setResponseList(responseManager.get());
        return lambdaResponse;
    }




}