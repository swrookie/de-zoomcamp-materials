package com.swrookie.producer;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import com.swrookie.dao.RideDAO;

public class JsonProducer {
    public List<RideDAO> getRides() throws IOException, CsvException {
        var rideStream = this.getClass().getResource("/fhv_tripdata_2019-01.csv.gz");
        var reader = new CSVReader(new FileReader(rideStream.getFile()));
        reader.skip(1);

        return reader.readAll().stream().map(arr -> new RideDAO(arr)).collect(Collectors.toList());
    }
}
