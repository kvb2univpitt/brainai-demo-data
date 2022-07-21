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

import edu.pitt.dbmi.brainai.demo.data.utils.FileUtils;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 *
 * Jul 20, 2022 5:44:12 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public class ReducedMapFromSyntheaTest {

    @TempDir
    public static Path tempDir;

    /**
     * Test of main method, of class ReducedMapFromSynthea.
     */
    @Test
    public void testMain() throws IOException {
        String dir = ReducedMapFromSyntheaTest.class.getResource("/data/synthea").getFile();
        String outDir = FileUtils.createSubDir(tempDir, "synthea").toString();
        ReducedMapFromSynthea.main(new String[]{dir, outDir});
    }

}
