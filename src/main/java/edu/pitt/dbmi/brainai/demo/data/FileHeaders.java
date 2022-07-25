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
        "PERSON_ID",
        "birth_dt_tm",
        "name_last",
        "name_first",
        "sex",
        "street_addr",
        "city",
        "state",
        "zipcode"
    };
    public static String[] ENCOUNTER = {
        "ENCNTR_ID",
        "REG_DT_TM",
        "DISCH_DT_TM",
        "PERSON_ID",
        "ENCNTR_TYPE_CD",
        "code_value.display",
        "snomed_code",
        "REASON_FOR_VISIT"
    };
    public static String[] OBSERVATION = {
        "OBSERV_ID",
        "event_end_dt_tm",
        "PERSON_ID",
        "ENCNTR_ID",
        "loinc_code",
        "loinc_description",
        "result_val",
        "result_units_cd",
        "Clinical Event Result Type",
        "category.code"
    };
    public static String[] DIAGNOSTIC_REPORT = {
        "issue_dt_tm",
        "effective_dt_tm",
        "PERSON_ID",
        "ENCNTR_ID",
        "OBSERV_ID"
    };

    private FileHeaders() {
    }

}
