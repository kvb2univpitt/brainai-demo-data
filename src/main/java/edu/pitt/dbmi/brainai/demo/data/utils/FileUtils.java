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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * Jul 20, 2022 2:32:39 PM
 *
 * @author Kevin V. Bui (kvb2univpitt@gmail.com)
 */
public final class FileUtils {

    private FileUtils() {
    }

    public static List<Path> listFiles(Path dir) throws IOException {
        try (Stream<Path> stream = java.nio.file.Files.walk(dir)) {
            return stream.map(Path::normalize)
                    .filter(java.nio.file.Files::isRegularFile)
                    .collect(Collectors.toList());

        }
    }

    public static Path createSubDir(Path dir, String name) throws IOException {
        return Files.createDirectory(Paths.get(dir.toString(), name));
    }

    public static List<String> readFileLineByLine(Path file) throws IOException {
        return Files.readAllLines(file);
    }

    public static void printOutputFiles(String outDir) throws IOException {
        listFiles(Paths.get(outDir))
                .forEach(file -> {
                    System.out.println("================================================================================");
                    System.out.println(file);
                    System.out.println("--------------------------------------------------------------------------------");
                    try {
                        readFileLineByLine(file)
                                .forEach(System.out::println);
                    } catch (IOException exception) {
                        exception.printStackTrace(System.err);
                    }
                    System.out.println("================================================================================");
                    System.out.println();
                });
    }

}
