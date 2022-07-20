/*
 * Copyright (C) 2022 University of Pittsburgh.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package edu.pitt.dbmi.brainai.demo.data.synthea;

import edu.pitt.dbmi.brainai.demo.data.FileHeaders;
import edu.pitt.dbmi.brainai.demo.data.utils.DateFormats;
import edu.pitt.dbmi.brainai.demo.data.utils.FhirUtils;
import edu.pitt.dbmi.brainai.demo.data.utils.FileUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Type;

/**
 * Map Synthea data to custom Cerner data for the Brain AI project.
 *
 * Jul 20, 2022 11:35:12 AM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class MapFromSynthea {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Path dataDir = Paths.get(args[0]);
        Path outDir = Paths.get(args[1]);
        System.out.println("================================================================================");
        System.out.println("Map From Synthea Data");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Data Directory: %s%n", dataDir.toString());
        System.out.printf("Output Directory: %s%n", outDir.toString());
        System.out.println();
        try {
            map(dataDir, outDir);
        } catch (IOException exception) {
            exception.printStackTrace(System.err);
        }
        System.out.println("================================================================================");
    }

    private static void map(Path dataDir, Path outDir) throws IOException {
        String dirOut = outDir.toString();
        try (PrintWriter patientWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "patients.tsv")));
                PrintWriter encounterWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "encounters.tsv")));
                PrintWriter observationWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "observations.tsv")))) {
            // write out headers
            patientWriter.println(Arrays.stream(FileHeaders.PATIENT).collect(Collectors.joining("\t")));
            encounterWriter.println(Arrays.stream(FileHeaders.ENCOUNTER).collect(Collectors.joining("\t")));
            observationWriter.println(Arrays.stream(FileHeaders.OBSERVATION).collect(Collectors.joining("\t")));

            // write out data
            for (Path file : FileUtils.listFiles(dataDir)) {
                try (BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset())) {
                    Bundle bundle = (Bundle) FhirUtils.JSON_PARSER.parseResource(reader);
                    extractPatient(bundle, patientWriter);
                    extractEncounter(bundle, encounterWriter);
                    extractObservation(bundle, observationWriter);
                }
            }
        }
    }

    private static void extractObservation(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Observation"))
                .map(e -> (Observation) e.getResource())
                .forEach(observation -> {
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(observation.getEffectiveDateTimeType().getValue()));
                    data.add(observation.getSubject().getReference().replace("urn:uuid:", ""));
                    data.add(observation.getEncounter().getReference().replace("urn:uuid:", ""));
                    data.add(observation.getCode().getCodingFirstRep().getCode());
                    data.add(observation.getCode().getCodingFirstRep().getDisplay());

                    Type type = observation.getValue();
                    if (type == null || !(type instanceof Quantity)) {
                        data.add("");
                        data.add("");
                        data.add("");
                    } else {
                        data.add(observation.getValueQuantity().getValue().toString());
                        data.add(observation.getValueQuantity().getUnit());
                        data.add("numeric");
                    }
                    data.add("laboratory");

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    data.clear();
                });
    }

    private static void extractEncounter(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Encounter"))
                .map(e -> (Encounter) e.getResource())
                .forEach(encounter -> {
                    data.add(encounter.getIdElement().getIdPart().replace("urn:uuid:", ""));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getStart()));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getEnd()));
                    data.add(encounter.getSubject().getReference().replace("urn:uuid:", ""));
                    data.add("394656005");
                    data.add("Inpatient");
                    data.add("126598008");
                    data.add("Neoplasm of connective tissues disorder");

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    data.clear();
                });
    }

    private static void extractPatient(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Patient"))
                .map(e -> (Patient) e.getResource())
                .forEach(patient -> {
                    data.add(patient.getIdElement().getIdPart().replace("urn:uuid:", ""));
                    data.add(DateFormats.MM_DD_YYYY.format(patient.getBirthDate()));
                    data.add(patient.getNameFirstRep().getFamily());
                    data.add(patient.getNameFirstRep().getGiven().get(0).getValueAsString());
                    data.add(FhirUtils.getValue(patient.getGender().toCode(), "female"));
                    data.add(FhirUtils.getValue(patient.getAddressFirstRep().getText(), "4200 Fifth Ave"));
                    data.add(FhirUtils.getValue(patient.getAddressFirstRep().getCity(), "Pittsburgh"));
                    data.add(FhirUtils.getValue(patient.getAddressFirstRep().getState(), "Pennsylvania"));
                    data.add(FhirUtils.getValue(patient.getAddressFirstRep().getPostalCode(), "15260"));

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    data.clear();
                });
    }

}
