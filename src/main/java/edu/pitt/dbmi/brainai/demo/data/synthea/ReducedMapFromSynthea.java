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
import static edu.pitt.dbmi.brainai.demo.data.synthea.AbstractSyntheaDataMapper.getBundle;
import static edu.pitt.dbmi.brainai.demo.data.synthea.AbstractSyntheaDataMapper.toLineHeader;
import edu.pitt.dbmi.brainai.demo.data.utils.DateFormats;
import edu.pitt.dbmi.brainai.demo.data.utils.FileUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Type;

/**
 * Map a certain amount of Synthea data to custom Cerner data for the Brain AI
 * project.
 *
 * Jul 20, 2022 5:43:15 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class ReducedMapFromSynthea extends AbstractSyntheaDataMapper {

    private static final int MAX_NUM_PATIENTS = 10;
    private static final int MAX_NUM_ENCOUNTERS = 4;
    private static final int MAX_NUM_OBSERVATIONS = 5;
    private static final int MAX_NUM_MEDICATION_ADMINISTRATION = 10;

    private static int totalNumOfPatients = 0;
    private static int totalNumOfEncounters = 0;
    private static int totalNumOfObservations = 0;
    private static int totalNumOfMedicationAdministrations = 0;

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
                PrintWriter medicationAdministrationWriter = new PrintWriter(Files.newOutputStream(Paths.get(dirOut, "medication_administrations.tsv")))) {
            // write out headers
            patientWriter.println(toLineHeader(FileHeaders.PATIENT));
            encounterWriter.println(toLineHeader(FileHeaders.ENCOUNTER));
            observationWriter.println(toLineHeader(FileHeaders.OBSERVATION));
            medicationAdministrationWriter.println(toLineHeader(FileHeaders.MEDICATION_ADMINISTRATION));

            // write out data
            int count = 0;
            for (Path file : FileUtils.listFiles(dataDir)) {
                if (count >= MAX_NUM_PATIENTS) {
                    break;
                }

                Bundle bundle = getBundle(file);
                List<String> data = new LinkedList<>();

                Patient patient = getPatient(bundle);
                exportPatient(patient, data, patientWriter);
                if (hasMedicationAdministration(bundle)) {
                    Map<String, Encounter> encountersFromIds = getEncountersFromIds(bundle);
                    Map<String, List<Observation>> encounterObservations = getEncounterObservations(bundle);
                    Map<String, List<MedicationAdministration>> encounterMedicationAdministrations = getEncounterMedicationAdministrations(bundle);

                    List<Encounter> exportedEncounters = exportEncounters(encountersFromIds, encounterObservations, encounterMedicationAdministrations, data, encounterWriter);
                    exportObservations(encounterObservations, exportedEncounters, data, observationWriter);
                    exportMedicationAdministrations(encounterMedicationAdministrations, exportedEncounters, data, medicationAdministrationWriter);
                } else {
                    Map<String, Encounter> encountersFromIds = getEncountersFromIds(bundle);
                    Map<String, List<Observation>> encounterObservations = getEncounterObservations(bundle);

                    List<Encounter> exportedEncounters = exportEncounters(encountersFromIds, encounterObservations, data, encounterWriter);
                    exportObservations(encounterObservations, exportedEncounters, data, observationWriter);
                }

                count++;
                totalNumOfPatients++;
            }
        }

        System.out.printf("Patients: %d%n", totalNumOfPatients);
        System.out.printf("Encounters: %d%n", totalNumOfEncounters);
        System.out.printf("Observations: %d%n", totalNumOfObservations);
        System.out.printf("Medication Administration: %d%n", totalNumOfMedicationAdministrations);
    }

    private static boolean hasMedicationAdministration(Bundle bundle) {
        Bundle.BundleEntryComponent entryComponent = bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("MedicationAdministration"))
                .findFirst().orElse(null);

        return (entryComponent != null);
    }

    private static List<MedicationAdministration> exportMedicationAdministrations(
            Map<String, List<MedicationAdministration>> encounterMedicationAdministrations,
            List<Encounter> exportedEncounters,
            List<String> data,
            PrintWriter writer) {
        List<MedicationAdministration> exportedMedicationAdministrations = new LinkedList<>();

        for (Encounter encounter : exportedEncounters) {
            String encounterId = encounter.getIdElement().getIdPart();

            int count = 0;
            for (MedicationAdministration medicationAdministration : encounterMedicationAdministrations.get(encounterId)) {
                if (count >= MAX_NUM_MEDICATION_ADMINISTRATION) {
                    break;
                }

                export(medicationAdministration, data, writer);
                exportedMedicationAdministrations.add(medicationAdministration);
                count++;
            }
            totalNumOfMedicationAdministrations += count;
        }

        return exportedMedicationAdministrations;
    }

    private static List<Observation> exportObservations(
            Map<String, List<Observation>> encounterObservations,
            List<Encounter> exportedEncounters,
            List<String> data,
            PrintWriter writer) {
        List<Observation> exportedObservations = new LinkedList<>();

        for (Encounter encounter : exportedEncounters) {
            String encounterId = encounter.getIdElement().getIdPart();

            int count = 0;
            for (Observation observation : encounterObservations.get(encounterId)) {
                if (count >= MAX_NUM_OBSERVATIONS) {
                    break;
                }

                export(observation, data, writer);
                exportedObservations.add(observation);
                count++;
            }
            totalNumOfObservations += count;
        }

        return exportedObservations;
    }

    private static List<Encounter> exportEncounters(
            Map<String, Encounter> encountersFromIds,
            Map<String, List<Observation>> encounterObservations,
            Map<String, List<MedicationAdministration>> encounterMedicationAdministrations,
            List<String> data,
            PrintWriter writer) {
        List<Encounter> exportedEncounters = new LinkedList<>();

        int count = 0;
        for (String encounterId : encounterMedicationAdministrations.keySet()) {
            if (count >= MAX_NUM_ENCOUNTERS) {
                break;
            }

            if (encounterObservations.containsKey(encounterId)) {
                Encounter encounter = encountersFromIds.get(encounterId);
                exportedEncounters.add(encounter);
                export(encounter, data, writer);
                count++;
            }
        }
        totalNumOfEncounters += count;

        return exportedEncounters;
    }

    private static List<Encounter> exportEncounters(
            Map<String, Encounter> encountersFromIds,
            Map<String, List<Observation>> encounterObservations,
            List<String> data,
            PrintWriter writer) {
        List<Encounter> exportedEncounters = new LinkedList<>();

        int count = 0;
        for (String encounterId : encounterObservations.keySet()) {
            if (count >= MAX_NUM_ENCOUNTERS) {
                break;
            }

            Encounter encounter = encountersFromIds.get(encounterId);
            exportedEncounters.add(encounter);
            export(encounter, data, writer);
            count++;

        }
        totalNumOfEncounters += count;

        return exportedEncounters;
    }

    private static void exportPatient(Patient patient, List<String> data, PrintWriter writer) {
        export(patient, data, writer);;
    }

    private static void export(MedicationAdministration medicationAdministration, List<String> data, PrintWriter writer) {
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
    }

    private static void export(Observation observation, List<String> data, PrintWriter writer) {
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
    }

    private static void export(Encounter encounter, List<String> data, PrintWriter writer) {
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
    }

    private static void export(Patient patient, List<String> data, PrintWriter writer) {
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

        writer.println(data.stream().collect(Collectors.joining(DATA_DELIMITER)));
        data.clear();
    }

    private static Map<String, List<MedicationAdministration>> getEncounterMedicationAdministrations(Bundle bundle) {
        Map<String, List<MedicationAdministration>> encounterMedicationAdministrations = new HashMap<>();

        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("MedicationAdministration"))
                .map(e -> (MedicationAdministration) e.getResource())
                .forEach(medicationAdministration -> {
                    String encounterId = medicationAdministration.getContext().getReference();
                    List<MedicationAdministration> medicationAdministrations = encounterMedicationAdministrations.get(encounterId);
                    if (medicationAdministrations == null) {
                        medicationAdministrations = new LinkedList<>();
                        encounterMedicationAdministrations.put(encounterId, medicationAdministrations);
                    }
                    medicationAdministrations.add(medicationAdministration);
                });

        return encounterMedicationAdministrations;
    }

    private static Map<String, List<Observation>> getEncounterObservations(Bundle bundle) {
        Map<String, List<Observation>> encounterObservations = new HashMap<>();

        bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Observation"))
                .map(e -> (Observation) e.getResource())
                .forEach(observation -> {
                    String encounterId = observation.getEncounter().getReference();
                    List<Observation> observations = encounterObservations.get(encounterId);
                    if (observations == null) {
                        observations = new LinkedList<>();
                        encounterObservations.put(encounterId, observations);
                    }
                    observations.add(observation);
                });

        return encounterObservations;
    }

    private static Map<String, Encounter> getEncountersFromIds(Bundle bundle) {
        return bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Encounter"))
                .map(e -> (Encounter) e.getResource())
                .collect(Collectors.toMap(e -> e.getIdElement().getIdPart(), e -> e));
    }

    private static List<Observation> getObservations(Bundle bundle) {
        return bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Observation"))
                .map(e -> (Observation) e.getResource())
                .collect(Collectors.toList());
    }

    private static List<Encounter> getEncounters(Bundle bundle) {
        return bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Encounter"))
                .map(e -> (Encounter) e.getResource())
                .collect(Collectors.toList());
    }

    private static Patient getPatient(Bundle bundle) {
        return bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("Patient"))
                .map(e -> (Patient) e.getResource())
                .findFirst().orElse(null);
    }

}
