package org.bootcamp.latam.model;

import java.util.List;

public class LambdaResponse {
    private List<Response> responseList;

    public LambdaResponse() {
    }

    public List<Response> getResponseList() {
        return responseList;
    }

    public void setResponseList(List<Response> responseList) {
        this.responseList = responseList;
    }
}
