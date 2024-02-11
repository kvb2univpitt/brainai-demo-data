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
import edu.pitt.dbmi.brainai.demo.data.utils.FileUtils;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeableConcept;
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
 * Map a certain amount of Synthea data to custom Cerner data for the Brain AI
 * project.
 *
 * Jul 20, 2022 5:43:15 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class ReducedMapFromSynthea extends AbstractSyntheaDataMapper {

    private static final int MAX_NUM_PATIENTS = Integer.MAX_VALUE;
    private static final int MAX_NUM_ENCOUNTERS = Integer.MAX_VALUE;
    private static final int MAX_NUM_OBSERVATIONS = Integer.MAX_VALUE;
    private static final int MAX_NUM_MEDICATION_ADMINISTRATION = Integer.MAX_VALUE;

    private static int totalNumOfPatients = 0;
    private static int totalNumOfEncounters = 0;
    private static int totalNumOfObservations = 0;
    private static int totalNumOfMedicationAdministrations = 0;
    private static int totalNumOfLocations = 0;
    private static int totalNumOfEncounterLocations = 0;

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
        System.out.printf("Patients: %d%n", totalNumOfPatients);
        System.out.printf("Encounters: %d%n", totalNumOfEncounters);
        System.out.printf("Observations: %d%n", totalNumOfObservations);
        System.out.printf("Medication Administration: %d%n", totalNumOfMedicationAdministrations);
        System.out.printf("Locations: %d%n", totalNumOfLocations);
        System.out.printf("Encounter Locations: %d%n", totalNumOfEncounterLocations);
        System.out.println("================================================================================");
    }

    private static void map(Path dataDir, Path outDir) throws IOException {
        List<Bundle> bundles = getBundles(dataDir);
        Map<String, Patient> patients = getPatients(bundles);
        Map<String, List<Encounter>> patientEncounters = getPatientEncounters(bundles, patients.keySet());
        Map<String, List<Observation>> encounterObservations = getEncounterObservations(bundles, patientEncounters);
        Map<String, List<MedicationAdministration>> encounterMedicationAdministrations = getEncounterMedicationAdministrations(bundles, encounterObservations);
        List<Organization> organizations = getOrganization(bundles, patientEncounters);

        List<String> data = new LinkedList<>();  // used for construction line for export
        exportPatients(patients, data, outDir);
        exportEncounters(patientEncounters, data, outDir);
        exportObservations(encounterObservations, data, outDir);
        exportMedicationAdministration(encounterMedicationAdministrations, data, outDir);
        exportLocationDerivedFromOrganizations(organizations, data, outDir);
        exportEncounterLocations(patientEncounters, data, outDir);
    }

    private static void exportEncounterLocations(Map<String, List<Encounter>> patientEncounters, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "encounter_locations.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.ENCOUNTER_LOCATION));

            for (List<Encounter> encounters : patientEncounters.values()) {
                for (Encounter encounter : encounters) {
                    Date start = encounter.getPeriod().getStart();
                    Date end = encounter.getPeriod().getEnd();
                    String encounterId = getCustomEncounterId(encounter);
                    String locationId = getCustomLocationId(encounter);

                    data.clear();
                    data.add(encounterId);
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(start));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(end));
                    data.add(locationId);
                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    totalNumOfEncounterLocations++;
                }
            }
        }
    }

    private static void exportLocationDerivedFromOrganizations(List<Organization> organizations, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "locations.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.LOCATION));

            for (Organization organization : organizations) {
                data.clear();
                data.add(createCustomLocationId(organization.getIdElement()));
                data.add(organization.getName());

                Address address = organization.getAddressFirstRep();
                String addressLine = address.getLine().isEmpty()
                        ? "4200 Fifth Ave"
                        : address.getLine().get(0).asStringValue();
                data.add(addressLine);
                data.add(address.getCity());
                data.add(address.getState());
                data.add(address.getPostalCode());
                data.add(Location.LocationStatus.ACTIVE.toString());

                data.add(organization.getTypeFirstRep().getCodingFirstRep().getCode());
                data.add(organization.getTypeFirstRep().getCodingFirstRep().getSystem());
                data.add(organization.getTypeFirstRep().getCodingFirstRep().getDisplay());

                writer.println(data.stream().collect(Collectors.joining("\t")));
                totalNumOfLocations++;
            }
        }
    }

    private static void exportMedicationAdministration(Map<String, List<MedicationAdministration>> encounterMedicationAdministrations, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "medication_administrations.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.MEDICATION_ADMINISTRATION));

            for (List<MedicationAdministration> medicationAdministrations : encounterMedicationAdministrations.values()) {
                for (MedicationAdministration medicationAdministration : medicationAdministrations) {
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
                    totalNumOfMedicationAdministrations++;
                }
            }
        }
    }

    private static void exportObservations(Map<String, List<Observation>> encounterObservations, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "observations.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.OBSERVATION));

            for (List<Observation> observations : encounterObservations.values()) {
                for (Observation observation : observations) {
                    data.clear();
                    data.add(getCustomObservationId(observation));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(observation.getEffectiveDateTimeType().getValue()));
                    data.add(getCustomPatientId(observation));
                    data.add(getCustomEncounterId(observation));
                    data.add(observation.getCode().getCodingFirstRep().getCode());
                    data.add(observation.getCode().getCodingFirstRep().getDisplay());

                    Type type = observation.getValue();
                    if (type == null) {
                        type = observation.getComponentFirstRep().getValue();
                        if (type instanceof Quantity) {
                            Quantity quantity = observation.getComponentFirstRep().getValueQuantity();
                            data.add(quantity.getValue().toString());
                            data.add(quantity.getUnit());
                            data.add("numeric");
                        } else {
                            data.add("");
                            data.add("");
                            data.add("");
                        }
                    } else {
                        if (type instanceof Quantity) {
                            data.add(observation.getValueQuantity().getValue().toString());
                            data.add(observation.getValueQuantity().getUnit());
                            data.add("numeric");
                        } else {
                            data.add("");
                            data.add("");
                            data.add("");
                        }
                    }
                    data.add(observation.getCategoryFirstRep().getCodingFirstRep().getDisplay());
                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    totalNumOfObservations++;
                }
            }
        }
    }

    private static void exportEncounters(Map<String, List<Encounter>> patientEncounters, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "encounters.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.ENCOUNTER));

            for (List<Encounter> encounters : patientEncounters.values()) {
                for (Encounter encounter : encounters) {
                    data.clear();
                    data.add(getCustomEncounterId(encounter));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getStart()));
                    data.add(DateFormats.MM_DD_YYYY_HHMMSS_AM.format(encounter.getPeriod().getEnd()));
                    data.add(getCustomPatientId(encounter));

                    CodeableConcept type = encounter.getTypeFirstRep();
                    String typeCode = type.getCodingFirstRep().getCode();
                    String typeDisplay = type.getCodingFirstRep().getDisplay();
                    data.add(getValue(typeCode, "394656005"));
                    data.add(getValue(typeDisplay, "Inpatient"));

                    CodeableConcept reason = encounter.getReasonCodeFirstRep();
                    String reasonCode = reason.getCodingFirstRep().getCode();
                    String reasonDisplay = reason.getCodingFirstRep().getDisplay();
                    data.add(getValue(reasonCode, "126598008"));
                    data.add(getValue(reasonDisplay, "Neoplasm of connective tissues disorder"));
                    data.add(getCustomLocationId(encounter));
                    data.add(encounter.getServiceProvider().getDisplay());

                    writer.println(data.stream().collect(Collectors.joining("\t")));
                    totalNumOfEncounters++;
                }
            }
        }
    }

    private static void exportPatients(Map<String, Patient> patients, List<String> data, Path outDir) throws IOException {
        try (PrintWriter writer = new PrintWriter(Files.newOutputStream(Paths.get(outDir.toString(), "patients.tsv")))) {
            // write out header
            writer.println(toLineHeader(FileHeaders.PATIENT));

            for (Patient patient : patients.values()) {
                data.clear();
                data.add(getCustomPatientId(patient));
                data.add(DateFormats.MM_DD_YYYY.format(patient.getBirthDate()));
                data.add(patient.getNameFirstRep().getFamily());
                data.add(patient.getNameFirstRep().getGiven().get(0).getValueAsString());
                data.add(getValue(patient.getGender().toCode(), "female"));

                Address address = patient.getAddressFirstRep();
                String addressLine = address.getLine().isEmpty()
                        ? "4200 Fifth Ave"
                        : address.getLine().get(0).asStringValue();
                data.add(addressLine);
                data.add(getValue(patient.getAddressFirstRep().getCity(), "Pittsburgh"));
                data.add(getValue(patient.getAddressFirstRep().getState(), "Pennsylvania"));
                data.add(getValue(patient.getAddressFirstRep().getPostalCode(), "15260"));
                data.add(getValue(patient.getAddressFirstRep().getCountry(), "US"));

                writer.println(data.stream().collect(Collectors.joining(DATA_DELIMITER)));
                totalNumOfPatients++;
            }
        }
    }

    private static List<Organization> getOrganization(List<Bundle> bundles, Map<String, List<Encounter>> patientEncounters) {
        List<Organization> organizations = new LinkedList<>();

        // get unique organization IDs from encounters
        Set<String> organizationIds = new HashSet<>();
        for (List<Encounter> encounters : patientEncounters.values()) {
            encounters.forEach(encounter -> {
                organizationIds.add(encounter.getServiceProvider().getReferenceElement().getIdPart().replaceAll("synthea\\|", ""));
            });
        }

        for (Bundle bundle : bundles) {
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if (entry.getResource().fhirType().equals("Organization")) {
                    Organization organization = (Organization) entry.getResource();
                    String organizationId = organization.getIdElement().getIdPart().replaceAll("urn:uuid:", "");
                    if (organizationIds.contains(organizationId)) {
                        organizations.add(organization);
                    }
                }
            }
        }

        return organizations;
    }

    private static Map<String, List<MedicationAdministration>> getEncounterMedicationAdministrations(List<Bundle> bundles, Map<String, List<Observation>> encounterObservations) {
        Map<String, List<MedicationAdministration>> encounterMedicationAdministrations = new HashMap<>();

        // initialize encounter-medication-administration list with empty lists
        for (String encounterId : encounterObservations.keySet()) {
            encounterMedicationAdministrations.put(encounterId, new LinkedList<>());
        }

        for (Bundle bundle : bundles) {
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if (entry.getResource().fhirType().equals("MedicationAdministration")) {
                    MedicationAdministration medicationAdministration = (MedicationAdministration) entry.getResource();
                    String encounterId = medicationAdministration.getContext().getReference();
                    List<MedicationAdministration> medicationAdministrations = encounterMedicationAdministrations.get(encounterId);
                    if (medicationAdministrations != null && medicationAdministrations.size() < MAX_NUM_MEDICATION_ADMINISTRATION) {
                        medicationAdministrations.add(medicationAdministration);
                    }
                }
            }
        }

        return encounterMedicationAdministrations;
    }

    private static Map<String, List<Observation>> getEncounterObservations(List<Bundle> bundles, Map<String, List<Encounter>> patientEncounters) {
        Map<String, List<Observation>> encounterObservations = new HashMap<>();

        // initialize encounter-observation list with empty lists
        for (List<Encounter> encounters : patientEncounters.values()) {
            encounters.forEach(encounter -> encounterObservations.put(encounter.getIdElement().getIdPart(), new LinkedList<>()));
        }

        for (Bundle bundle : bundles) {
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if (entry.getResource().fhirType().equals("Observation")) {
                    Observation observation = (Observation) entry.getResource();
                    String encounterId = observation.getEncounter().getReference();
                    List<Observation> observations = encounterObservations.get(encounterId);
                    if (observations != null && observations.size() < MAX_NUM_OBSERVATIONS) {
                        observations.add(observation);
                    }
                }
            }
        }

        return encounterObservations;
    }

    private static Map<String, List<Encounter>> getPatientEncounters(List<Bundle> bundles, Set<String> patientIds) {
        Map<String, List<Encounter>> patientEncounters = new HashMap<>();

        // initialize patient-encounter list with empty lists
        patientIds.forEach(patientId -> patientEncounters.put(patientId, new LinkedList<>()));

        for (Bundle bundle : bundles) {
            // get encounter IDs of encounters that have medication adminstration
            Set<String> medAdminEncounterIds = bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("MedicationAdministration"))
                    .map(e -> (MedicationAdministration) e.getResource())
                    .map(e -> e.getContext().getReference())
                    .collect(Collectors.toSet());

            // get encounter IDs of encounters that have observations without medication adminstration
            Set<String> observationEncounterIds = bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("Observation"))
                    .map(e -> (Observation) e.getResource())
                    .map(e -> e.getEncounter().getReference())
                    .filter(e -> !medAdminEncounterIds.contains(e))
                    .collect(Collectors.toSet());

            // extract encounters
            List<Encounter> medAdminEncounters = new LinkedList<>();
            List<Encounter> observationEncounters = new LinkedList<>();
            List<Encounter> regularEncounters = new LinkedList<>();
            for (Bundle.BundleEntryComponent entry : bundle.getEntry()) {
                if (entry.getResource().fhirType().equals("Encounter")) {
                    Encounter encounter = (Encounter) entry.getResource();
                    String encounterId = encounter.getIdElement().getIdPart();
                    if (medAdminEncounterIds.contains(encounterId)) {
                        medAdminEncounters.add(encounter);
                    } else if (observationEncounterIds.contains(encounterId)) {
                        observationEncounters.add(encounter);
                    } else {
                        regularEncounters.add(encounter);
                    }
                }
            }

            for (Encounter encounter : medAdminEncounters) {
                String patientId = encounter.getSubject().getReference();
                List<Encounter> encounters = patientEncounters.get(patientId);
                if (encounters != null && encounters.size() < MAX_NUM_ENCOUNTERS) {
                    encounters.add(encounter);
                }
            }
            for (Encounter encounter : observationEncounters) {
                String patientId = encounter.getSubject().getReference();
                List<Encounter> encounters = patientEncounters.get(patientId);
                if (encounters != null && encounters.size() < MAX_NUM_ENCOUNTERS) {
                    encounters.add(encounter);
                }
            }
            for (Encounter encounter : regularEncounters) {
                String patientId = encounter.getSubject().getReference();
                List<Encounter> encounters = patientEncounters.get(patientId);
                if (encounters != null && encounters.size() < MAX_NUM_ENCOUNTERS) {
                    encounters.add(encounter);
                }
            }
        }

        return patientEncounters;
    }

    private static Map<String, Patient> getPatients(List<Bundle> bundles) {
        Map<String, Patient> patients = new HashMap<>();

        int count = 0;
        for (Bundle bundle : bundles) {
            if (count >= MAX_NUM_PATIENTS) {
                break;
            }

            Patient patient = bundle.getEntry().stream()
                    .filter(e -> e.getResource().fhirType().equals("Patient"))
                    .map(e -> (Patient) e.getResource())
                    .findFirst().orElse(null);
            if (patient != null) {
                patients.put(patient.getIdElement().getIdPart(), patient);
                count++;
            }
        }

        return patients;
    }

    private static List<Bundle> getBundles(Path dataDir) throws IOException {
        List<Bundle> bundles = new LinkedList<>();
        for (Path file : FileUtils.listFiles(dataDir)) {
            bundles.add(getBundle(file));
        }

        return bundles;
    }

    private static boolean hasMedicationAdministration(Bundle bundle) {
        Bundle.BundleEntryComponent entryComponent = bundle.getEntry().stream()
                .filter(e -> e.getResource().fhirType().equals("MedicationAdministration"))
                .findFirst().orElse(null);

        return (entryComponent != null);
    }

}
