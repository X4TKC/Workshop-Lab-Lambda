package org.bootcamp.latam.handlers;

import org.bootcamp.latam.model.Param;
import org.bootcamp.latam.model.Publication;
import org.bootcamp.latam.model.Response;

import java.util.ArrayList;
import java.util.List;

public class ResponseManager {
    private List<Response> responseList;

    public ResponseManager() {
        responseList=new ArrayList<>();
    }

    public ResponseManager add(String question, List<Publication> responseQuery, int size){
        Response response = new Response();
        response.setQuestion(question);
        response.setResponse(responseQuery);
        response.setResponseSize(size);
        responseList.add(response);
        return this;
    }
    public List<Response> get(){
        return responseList;
    }
}
