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
package edu.pitt.dbmi.brainai.demo.data;

/**
 *
 * Jul 20, 2022 12:18:20 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public final class FileHeaders {

    public static String[] PATIENT = {
        "id",
        "birth_date",
        "last_name",
        "first_name",
        "gender",
        "address",
        "city",
        "state",
        "zip_code",
        "country"
    };
    public static String[] ENCOUNTER = {
        "id",
        "start",
        "end",
        "patient_id",
        "type_code",
        "type_display",
        "reason_code",
        "reason_display"
    };
    public static String[] OBSERVATION = {
        "id",
        "effective",
        "patient_id",
        "encounter_id",
        "code",
        "code_display",
        "component_value",
        "component_unit",
        "component_type",
        "category"
    };
    public static String[] MEDICATION_ADMINISTRATION = {
        "id",
        "status",
        "effective",
        "patient_id",
        "encounter_id",
        "medication_code",
        "medication_system",
        "medication_display"
    };

    public static String[] LOCATION = {
        "id",
        "name",
        "address",
        "city",
        "state",
        "zip_code",
        "status",
        "type_code",
        "type_system",
        "type_display"
    };

    public static String[] ENCOUNTER_LOCATION = {
        "encounter_id",
        "start",
        "end",
        "location_id"
    };

    private FileHeaders() {
    }

}
