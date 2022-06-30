package org.bootcamp.latam.service;

import lombok.SneakyThrows;
import software.amazon.awssdk.services.athena.AthenaClient;

import java.util.List;

public class AthenaOrchestrator<T>
{
    private final String query;
    private final Class<T> pojoClass;
    private final AthenaClient athenaClient;

    public AthenaOrchestrator(AthenaClient athenaClient, String query, Class<T> pojoClass) {
        this.query = query;
        this.pojoClass = pojoClass;
        this.athenaClient = athenaClient;
    }

    @SneakyThrows
    public  List<T> execute() {
        String queryExecutionId =
                AthenaQueryExecutor.submitAthenaQuery(athenaClient, this.query);
        try {
            AthenaQueryExecutor.waitForQueryToComplete(athenaClient, queryExecutionId);
            return AthenaQueryExecutor.processResultRows(
                    athenaClient, queryExecutionId, pojoClass);
        } catch(InterruptedException e) {
            System.out.println("Error");
        }

        return null;
    }
}