package org.bootcamp.latam.service;


import org.bootcamp.latam.model.Publication;

import java.util.List;

public interface IAthenaService {
    List<Publication> getDataFromAthena(String myQuery);
}