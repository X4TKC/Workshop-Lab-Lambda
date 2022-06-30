package org.bootcamp.latam.service;
import org.bootcamp.latam.model.Publication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import java.util.List;

@Service
public class AthenaServiceImpl<T> implements IAthenaService
{

    AthenaClient athenaClient=AthenaClient.builder().region(Region.US_EAST_1)
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

    @Override
    public List<Publication> getDataFromAthena(String myQuery){

        return new AthenaOrchestrator<>(athenaClient, myQuery, Publication.class).execute();
    }
}