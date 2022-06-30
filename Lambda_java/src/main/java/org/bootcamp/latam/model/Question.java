package org.bootcamp.latam.model;

import java.util.List;


public class Question {
    private String question;
    private List<Input> input;
    private List<String> output;

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public List<Input> getInput() {
        return input;
    }

    public void setInput(List<Input> input) {
        this.input = input;
    }

    public List<String> getOutput() {
        return output;
    }

    public void setOutput(List<String> output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return "Question{" +
                "question='" + question + '\'' +
                ", input=" + input +
                ", output=" + output +
                '}';
    }
}
