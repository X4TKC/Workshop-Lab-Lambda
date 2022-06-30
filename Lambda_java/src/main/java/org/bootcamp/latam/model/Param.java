package org.bootcamp.latam.model;

import java.util.List;
import java.util.Map;

public class Param {
    List<Question> questions;

    public Param() {
    }

    public List<Question> getQuestions() {
        return questions;
    }

    public void setQuestions(List<Question> questions) {
        this.questions = questions;
    }

    @Override
    public String toString() {
        return "Param{" +
                "questions=" + questions +
                '}';
    }
}
