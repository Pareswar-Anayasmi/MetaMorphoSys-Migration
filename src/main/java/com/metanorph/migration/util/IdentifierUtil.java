package com.metanorph.migration.util;

import org.apache.commons.csv.CSVRecord;
import java.util.List;
import java.util.Map;


public final class IdentifierUtil {

    private IdentifierUtil(){}

    public static String buildIdentifier(
            CSVRecord recordData,
            List<String> identifiers,
            Map<String,String> headerMap){

        // If identifier not configured, skip deduplication
        if(identifiers == null || identifiers.isEmpty()){
            return null;
        }

        StringBuilder key = new StringBuilder();

        for(String column : identifiers){

            String header = headerMap.get(column);

            if(header != null){
                key.append(recordData.get(header).trim()).append("|");
            }
        }

        return key.toString();
    }
}
