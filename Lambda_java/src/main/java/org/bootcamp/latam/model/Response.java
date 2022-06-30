package org.bootcamp.latam.model;

import java.util.List;

public class Response {
    private String question;
    private List<Publication> response;
    private int responseSize;

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public List<Publication> getResponse() {
        return response;
    }

    public void setResponse(List<Publication> response) {
        this.response = response;
    }

    public int getResponseSize() {
        return responseSize;
    }

    public void setResponseSize(int responseSize) {
        this.responseSize = responseSize;
    }

}
