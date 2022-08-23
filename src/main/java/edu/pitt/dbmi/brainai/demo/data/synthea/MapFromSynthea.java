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
import static edu.pitt.dbmi.brainai.demo.data.synthea.AbstractSyntheaDataMapper.getCustomEncounterId;
import static edu.pitt.dbmi.brainai.demo.data.synthea.AbstractSyntheaDataMapper.getCustomMedicationAdministrationId;
import static edu.pitt.dbmi.brainai.demo.data.synthea.AbstractSyntheaDataMapper.getCustomPatientId;
import edu.pitt.dbmi.brainai.demo.data.utils.DateFormats;
import edu.pitt.dbmi.brainai.demo.data.utils.FileUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Location;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Organization;
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
public class MapFromSynthea extends AbstractSyntheaDataMapper {

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
                PrintWriter observationWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "observations.tsv")));
                PrintWriter medicationAdministrationWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "medication_administrations.tsv")));
                PrintWriter locationWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "locations.tsv")))) {
            // write out headers
            patientWriter.println(toLineHeader(FileHeaders.PATIENT));
            encounterWriter.println(toLineHeader(FileHeaders.ENCOUNTER));
            observationWriter.println(toLineHeader(FileHeaders.OBSERVATION));
            medicationAdministrationWriter.println(toLineHeader(FileHeaders.MEDICATION_ADMINISTRATION));
            locationWriter.println(toLineHeader(FileHeaders.LOCATION));

            // write out data
            for (Path file : FileUtils.listFiles(dataDir)) {
                Bundle bundle = getBundle(file);
                extractPatient(bundle, patientWriter);
                extractEncounter(bundle, encounterWriter);
                extractObservation(bundle, observationWriter);
                extractMedicationAdministration(bundle, medicationAdministrationWriter);
                createLocation(bundle, locationWriter);
            }
        }
    }

    private static void createLocation(Bundle bundle, PrintWriter writer) {
        List<Organization> organizations = bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Organization"))
                .map(e -> (Organization) e.getResource()).collect(Collectors.toList());

        int count = 0;
        int limit = 3;
        List<String> data = new LinkedList<>();
        for (Organization organization : organizations) {
            data.add(createCustomLocationId(organization.getIdElement()));
            data.add(organization.getName());

            Address address = organization.getAddressFirstRep();
            data.add(address.getText());
            data.add(address.getCity());
            data.add(address.getState());
            data.add(address.getPostalCode());
            data.add(Location.LocationStatus.ACTIVE.toString());

            switch (count % limit) {
                case 0 -> {
                    data.add("INLAB");
                    data.add("inpatient laboratory");
                    data.add("A location that plays the role of delivering services which may include tests are done on clinical specimens to get health information about a patient pertaining to the diagnosis, treatment, and prevention of disease for a hospital visit longer than one day.");
                }
                case 1 -> {
                    data.add("PEDICU");
                    data.add("Pediatric intensive care unit");
                    data.add("");
                }
                default -> {
                    data.add("ICU");
                    data.add("Intensive care unit");
                    data.add("");
                }
            }

            writer.println(data.stream().collect(Collectors.joining("\t")));
            data.clear();
            count++;
        }
    }

    private static void extractMedicationAdministration(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("MedicationAdministration"))
                .map(e -> (MedicationAdministration) e.getResource())
                .forEach(medicationAdministration -> {
                    data.clear();
                    data.add(getCustomMedicationAdministrationId(medicationAdministration));
                    data.add(medicationAdministration.getStatus().getDisplay());
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(medicationAdministration.getEffectiveDateTimeType().getValue()));
                    data.add(getCustomPatientId(medicationAdministration));
                    data.add(getCustomEncounterId(medicationAdministration));

                    Coding medicationCoding = medicationAdministration.getMedicationCodeableConcept().getCodingFirstRep();
                    data.add(medicationCoding.getCode());
                    data.add(medicationCoding.getSystem());
                    data.add(medicationCoding.getDisplay());

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    data.clear();
                });
    }

    private static void extractObservation(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Observation"))
                .map(e -> (Observation) e.getResource())
                .forEach(observation -> {
                    data.clear();
                    data.add(getCustomObservationId(observation));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(observation.getEffectiveDateTimeType().getValue()));
                    data.add(getCustomPatientId(observation));
                    data.add(getCustomEncounterId(observation));
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
                    data.clear();
                    data.add(getCustomEncounterId(encounter));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getStart()));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getEnd()));
                    data.add(getCustomPatientId(encounter));
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
                    data.clear();
                    data.add(getCustomPatientId(patient));
                    data.add(DateFormats.MM_DD_YYYY.format(patient.getBirthDate()));
                    data.add(patient.getNameFirstRep().getFamily());
                    data.add(patient.getNameFirstRep().getGiven().get(0).getValueAsString());
                    data.add(getValue(patient.getGender().toCode(), "female"));
                    data.add(getValue(patient.getAddressFirstRep().getText(), "4200 Fifth Ave"));
                    data.add(getValue(patient.getAddressFirstRep().getCity(), "Pittsburgh"));
                    data.add(getValue(patient.getAddressFirstRep().getState(), "Pennsylvania"));
                    data.add(getValue(patient.getAddressFirstRep().getPostalCode(), "15260"));

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    data.clear();
                });
    }

}
