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

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;

/**
 *
 * Aug 2, 2022 1:11:19 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class AbstractSyntheaDataMapper {

    protected static final IParser JSON_PARSER = FhirContext.forR4().newJsonParser();

    /**
     * Maps Synethea ID to custom ID.
     */
    protected static final Map<String, String> syntheaToCustomPatientId = new HashMap<>();
    protected static final Map<String, String> syntheaToCustomEncounterId = new HashMap<>();
    protected static final Map<String, String> syntheaToCustomObservationId = new HashMap<>();

    private static int patientIdCounter = 0;
    private static int encounterIdCounter = 0;
    private static int observationIdCounter = 0;

    public AbstractSyntheaDataMapper() {
    }

    protected static boolean hasCustomEncounterId(Observation observation) {
        return syntheaToCustomEncounterId.containsKey(extractId(observation.getEncounter().getReference()));
    }

    protected static Bundle getBundle(Path file) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, Charset.defaultCharset())) {
            return (Bundle) JSON_PARSER.parseResource(reader);
        }
    }

    protected static String toLineHeader(String[] headers) {
        return Arrays.stream(headers).collect(Collectors.joining("\t"));
    }

    protected static String getValue(String value, String defaultValue) {
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    protected static String getCustomObservationId(Observation observation) {
        String id = extractId(observation.getIdElement().getIdPart());
        if (!syntheaToCustomObservationId.containsKey(id)) {
            syntheaToCustomObservationId.put(id, String.format("obs_%d", ++observationIdCounter));
        }

        return syntheaToCustomObservationId.get(id);
    }

    protected static String getCustomEncounterId(Observation observation) {
        String id = extractId(observation.getEncounter().getReference());
        if (!syntheaToCustomEncounterId.containsKey(id)) {
            System.err.printf("No encounter %s found for the observation.%n", id);
        }

        return syntheaToCustomEncounterId.get(id);
    }

    protected static String getCustomEncounterId(Encounter encounter) {
        String id = extractId(encounter.getIdElement().getIdPart());
        if (!syntheaToCustomEncounterId.containsKey(id)) {
            syntheaToCustomEncounterId.put(id, String.format("enc_%d", ++encounterIdCounter));
        }

        return syntheaToCustomEncounterId.get(id);
    }

    protected static String getCustomPatientId(Observation observation) {
        String id = extractId(observation.getSubject().getReference());
        if (!syntheaToCustomPatientId.containsKey(id)) {
            System.err.printf("No patient %s found for the observation.%n", id);
        }

        return syntheaToCustomPatientId.get(id);
    }

    protected static String getCustomPatientId(Encounter encounter) {
        String id = extractId(encounter.getSubject().getReference());
        if (!syntheaToCustomPatientId.containsKey(id)) {
            System.err.printf("No patient %s found for the encounter.%n", id);
        }

        return syntheaToCustomPatientId.get(id);
    }

    protected static String getCustomPatientId(Patient patient) {
        String id = extractId(patient.getIdElement().getIdPart());
        if (!syntheaToCustomPatientId.containsKey(id)) {
            syntheaToCustomPatientId.put(id, String.format("pat_%d", ++patientIdCounter));
        }

        return syntheaToCustomPatientId.get(id);
    }

    protected static String extractId(String resourceId) {
        return resourceId.replace("urn:uuid:", "");
    }

}
