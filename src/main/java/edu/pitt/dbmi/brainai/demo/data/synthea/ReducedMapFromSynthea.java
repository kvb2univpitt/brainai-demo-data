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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.Type;

/**
 * Map a certain amount of Synthea data to custom Cerner data for the Brain AI
 * project.
 *
 * Jul 20, 2022 5:43:15 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class ReducedMapFromSynthea {

    private static final int numOfPatients = 5;
    private static final int numOfEncounters = 10;
    private static final int numOfObservations = 10;
    private static final int numOfDiagnosticReport = 10;

    private static int totalPatients = 0;
    private static int totalEncounters = 0;
    private static int totalObservations = 0;
    private static int totalDiagnosticReports = 0;

    /**
     * Map Synthea patient ID (key) to Cerner patient ID (value).
     */
    private static final Map<String, String> patientIds = new HashMap<>();

    /**
     * Map Synthea encounter ID (key) to Cerner encounter ID (value)
     */
    private static final Map<String, String> encounterIds = new HashMap<>();

    /**
     * Map Synthea observation ID (key) to Cerner observation ID (value)
     */
    private static final Map<String, String> observationIds = new HashMap<>();

    /**
     * Map Synthea diagnostic report ID (key) to Cerner diagnostic report ID
     * (value)
     */
    private static final Map<String, String> diagnosticReportIds = new HashMap<>();

    private static int patientIdCount = 0;
    private static int encounterIdCount = 0;
    private static int observationIdCount = 0;
    private static int diagnosticReportIdCount = 0;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Path dataDir = Paths.get(args[0]);
        Path outDir = Paths.get(args[1]);
        System.out.println("================================================================================");
        System.out.println("Reduced Map From Synthea Data");
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
                PrintWriter diagnosticReportWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "diagnostic_report.tsv")))) {
            // write out headers
            patientWriter.println(Arrays.stream(FileHeaders.PATIENT).collect(Collectors.joining("\t")));
            encounterWriter.println(Arrays.stream(FileHeaders.ENCOUNTER).collect(Collectors.joining("\t")));
            observationWriter.println(Arrays.stream(FileHeaders.OBSERVATION).collect(Collectors.joining("\t")));
            diagnosticReportWriter.println(Arrays.stream(FileHeaders.DIAGNOSTIC_REPORT).collect(Collectors.joining("\t")));

            // write out data
            int count = 0;
            for (Path file : FileUtils.listFiles(dataDir)) {
                if (count >= numOfPatients) {
                    break;
                }

                try ( BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset())) {
                    Bundle bundle = (Bundle) FhirUtils.JSON_PARSER.parseResource(reader);
                    extractPatient(bundle, patientWriter);
                    extractEncounter(bundle, encounterWriter);
                    extractObservation(bundle, observationWriter);
                    extractDiagnosticReport(bundle, diagnosticReportWriter);

                    count++;
                    totalPatients++;
                }
            }

            System.out.printf("Patients: %d%n", totalPatients);
            System.out.printf("Encounters: %d%n", totalEncounters);
            System.out.printf("Observations: %d%n", totalObservations);
            System.out.printf("Diagnostic Reports: %d%n", totalDiagnosticReports);
        }
    }

    private static void extractDiagnosticReport(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        int count = 0;
        for (Bundle.BundleEntryComponent component : bundle.getEntry()) {
            if (count >= numOfDiagnosticReport) {
                break;
            }

            Resource resource = component.getResource();
            if (resource.fhirType().equals("DiagnosticReport")) {
                DiagnosticReport diagnosticReport = (DiagnosticReport) resource;

                String encounterID = diagnosticReport.getEncounter().getReference().replace("urn:uuid:", "");
                if (encounterIds.containsKey(encounterID)) {
                    List<Reference> references = diagnosticReport.getResult();
                    if (references.isEmpty()) {
                        data.add(getDiagnosticReportId(diagnosticReport));
                        data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(diagnosticReport.getIssued()));
                        data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(diagnosticReport.getEffectiveDateTimeType().getValue()));
                        data.add(getPatientId(diagnosticReport));
                        data.add(getEncounterId(diagnosticReport));
                        data.add("");

                        writer.println(data.stream().collect(Collectors.joining("\t")));
                        data.clear();
                    } else {
                        references.forEach(reference -> {
                            String observationID = reference.getReference().replace("urn:uuid:", "");
                            if (observationIds.containsKey(observationID)) {
                                data.add(getDiagnosticReportId(diagnosticReport));
                                data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(diagnosticReport.getIssued()));
                                data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(diagnosticReport.getEffectiveDateTimeType().getValue()));
                                data.add(getPatientId(diagnosticReport));
                                data.add(getEncounterId(diagnosticReport));
                                data.add(observationIds.get(observationID));

                                writer.println(data.stream().collect(Collectors.joining("\t")));
                                data.clear();
                            }
                        });
                    }

                    count++;
                    totalDiagnosticReports++;
                }
            }
        }
    }

    private static void extractObservation(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        int count = 0;
        for (Bundle.BundleEntryComponent component : bundle.getEntry()) {
            if (count >= numOfObservations) {
                break;
            }

            Resource resource = component.getResource();
            if (resource.fhirType().equals("Observation")) {
                Observation observation = (Observation) resource;

                String encounterID = observation.getEncounter().getReference().replace("urn:uuid:", "");
                if (encounterIds.containsKey(encounterID)) {
                    data.add(getObservationId(observation));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(observation.getEffectiveDateTimeType().getValue()));
                    data.add(getPatientId(observation));
                    data.add(getEncounterId(observation));
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

                    count++;
                    totalObservations++;
                }
            }
        }
    }

    private static void extractEncounter(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();

        int count = 0;
        for (Bundle.BundleEntryComponent component : bundle.getEntry()) {
            if (count >= numOfEncounters) {
                break;
            }

            Resource resource = component.getResource();
            if (resource.fhirType().equals("Encounter")) {
                Encounter encounter = (Encounter) resource;

                data.add(getEncounterId(encounter));
                data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getStart()));
                data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getEnd()));
                data.add(getPatientId(encounter));
                data.add("394656005");
                data.add("Inpatient");
                data.add("126598008");
                data.add("Neoplasm of connective tissues disorder");

                writer.println(data.stream().collect(Collectors.joining("\t")));
                data.clear();

                count++;
                totalEncounters++;
            }
        }
    }

    private static void extractPatient(Bundle bundle, PrintWriter writer) {
        List<String> data = new LinkedList<>();
        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Patient"))
                .map(e -> (Patient) e.getResource())
                .forEach(patient -> {
                    data.add(getPatientId(patient));
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

    /**
     * Map Synthea encounter ID to Cerner encounter ID from diagnostic report .
     *
     * @param observation
     * @return
     */
    private static String getEncounterId(DiagnosticReport diagnosticReport) {
        String id = diagnosticReport.getEncounter().getReference().replace("urn:uuid:", "");
        if (!encounterIds.containsKey(id)) {
            System.err.printf("No encounter %s found for the observation.%n", id);
        }

        return encounterIds.get(id);
    }

    /**
     * Map Synthea encounter ID to Cerner encounter ID.
     *
     * @param encounter
     * @return
     */
    private static String getDiagnosticReportId(DiagnosticReport diagnosticReport) {
        String id = diagnosticReport.getIdElement().getIdPart().replace("urn:uuid:", "");
        if (!diagnosticReportIds.containsKey(id)) {
            diagnosticReportIds.put(id, String.format("diag-report%d", ++diagnosticReportIdCount));
        }

        return diagnosticReportIds.get(id);
    }

    /**
     * Map Synthea encounter ID to Cerner encounter ID from observation.
     *
     * @param observation
     * @return
     */
    private static String getEncounterId(Observation observation) {
        String id = observation.getEncounter().getReference().replace("urn:uuid:", "");
        if (!encounterIds.containsKey(id)) {
            System.err.printf("No encounter %s found for the observation.%n", id);
        }

        return encounterIds.get(id);
    }

    private static String getObservationId(Observation observation) {
        String id = observation.getIdElement().getIdPart().replace("urn:uuid:", "");
        if (!observationIds.containsKey(id)) {
            observationIds.put(id, String.format("obs%d", ++observationIdCount));
        }

        return observationIds.get(id);
    }

    /**
     * Map Synthea encounter ID to Cerner encounter ID.
     *
     * @param encounter
     * @return
     */
    private static String getEncounterId(Encounter encounter) {
        String id = encounter.getIdElement().getIdPart().replace("urn:uuid:", "");
        if (!encounterIds.containsKey(id)) {
            encounterIds.put(id, String.format("enc%d", ++encounterIdCount));
        }

        return encounterIds.get(id);
    }

    /**
     * Map Synthea patient ID to Cerner patient ID from Synthea diagnostic
     * report.
     *
     * @param encounter
     * @return
     */
    private static String getPatientId(DiagnosticReport diagnosticReport) {
        String id = diagnosticReport.getSubject().getReference().replace("urn:uuid:", "");
        if (!patientIds.containsKey(id)) {
            System.err.printf("No patient %s found for the diagnostic report.%n", id);
        }

        return patientIds.get(id);
    }

    /**
     * Map Synthea patient ID to Cerner patient ID from Synthea observation.
     *
     * @param observation
     * @return
     */
    private static String getPatientId(Observation observation) {
        String id = observation.getSubject().getReference().replace("urn:uuid:", "");
        if (!patientIds.containsKey(id)) {
            System.err.printf("No patient %s found for the observation.%n", id);
        }

        return patientIds.get(id);
    }

    /**
     * Map Synthea patient ID to Cerner patient ID from Synthea encounter.
     *
     * @param encounter
     * @return
     */
    private static String getPatientId(Encounter encounter) {
        String id = encounter.getSubject().getReference().replace("urn:uuid:", "");
        if (!patientIds.containsKey(id)) {
            System.err.printf("No patient %s found for the encounter.%n", id);
        }

        return patientIds.get(id);
    }

    /**
     * Map Synthea patient ID to Cerner patient ID.
     *
     * @param patient
     * @return
     */
    private static String getPatientId(Patient patient) {
        String id = patient.getIdElement().getIdPart().replace("urn:uuid:", "");
        if (!patientIds.containsKey(id)) {
            patientIds.put(id, String.valueOf(++patientIdCount));
        }

        return patientIds.get(id);
    }

}
