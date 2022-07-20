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
package edu.pitt.dbmi.brainai.demo.data.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 *
 * Jul 20, 2022 12:16:09 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public final class DateFormats {

    public static final DateFormat MM_DD_YYYY = new SimpleDateFormat("MM/dd/yyyy");
    public static final DateFormat MM_DD_YYYY_HHMMSS_AM = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");

    private DateFormats() {
    }

}
