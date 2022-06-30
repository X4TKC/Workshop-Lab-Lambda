package org.bootcamp.latam.model;

public class Publication {
    private String title;
    private String author;
    private String type;
    private String language;
    private String dateedition;
    private String publication;

    public Publication() {
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getDateedition() {
        return dateedition;
    }

    public void setDateedition(String dateedition) {
        this.dateedition = dateedition;
    }

    public String getPublication() {
        return publication;
    }

    public void setPublication(String publication) {
        this.publication = publication;
    }
}
