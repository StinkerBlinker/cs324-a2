package cs324.assignment2.client;

import java.io.IOException;
import java.io.Reader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

final class CsvJobLoader {

    private CsvJobLoader() {}

    static List<Long> loadValues(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return parseValues(reader);
        }
    }

    /** Every CSV cell is a number. Rows are formatting, not separate jobs. */
    static List<Long> parseValues(Reader reader) throws IOException {
        List<List<String>> rows = readRecords(reader);
        List<Long> values = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) {
            List<String> cells = rows.get(row);
            for (int column = 0; column < cells.size(); column++) {
                String token = cells.get(column).trim();
                if (row == 0 && column == 0) token = token.replace("\uFEFF", "").trim();
                try {
                    values.add(Long.parseLong(token));
                } catch (NumberFormatException error) {
                    throw new IllegalArgumentException("CSV row " + (row + 1) + ", column "
                            + (column + 1) + ": expected a whole number. Use numeric values only, without job headers.", error);
                }
            }
        }
        if (values.isEmpty()) throw new IllegalArgumentException("CSV file contains no numbers.");
        return List.copyOf(values);
    }

    static List<Long> parseManualValues(String text) {
        if (text == null || text.isBlank()) throw new IllegalArgumentException("Enter at least one number.");
        String[] tokens = text.trim().split("[;,\\s]+");
        List<Long> values = new ArrayList<>(tokens.length);
        try {
            for (String token : tokens) values.add(Long.parseLong(token));
        } catch (NumberFormatException error) {
            throw new IllegalArgumentException("Enter whole numbers separated by commas, spaces or semicolons.", error);
        }
        return values;
    }

    private static List<List<String>> readRecords(Reader reader) throws IOException {
        if (!reader.markSupported()) {
            reader = new BufferedReader(reader);
        }
        List<List<String>> records = new ArrayList<>();
        List<String> record = new ArrayList<>();
        StringBuilder field = new StringBuilder();
        boolean quoted = false;
        boolean closedQuote = false;
        int current;

        while ((current = reader.read()) != -1) {
            char c = (char) current;
            if (quoted) {
                if (c == '"') {
                    reader.mark(1);
                    int next = reader.read();
                    if (next == '"') {
                        field.append('"');
                    } else {
                        quoted = false;
                        closedQuote = true;
                        if (next != -1) {
                            reader.reset();
                        }
                    }
                } else {
                    field.append(c);
                }
            } else if (c == ',' || c == '\n' || c == '\r') {
                record.add(field.toString());
                field.setLength(0);
                closedQuote = false;
                if (c != ',') {
                    if (c == '\r') {
                        reader.mark(1);
                        int next = reader.read();
                        if (next != '\n' && next != -1) {
                            reader.reset();
                        }
                    }
                    if (!(record.size() == 1 && record.get(0).isBlank())) {
                        records.add(record);
                    }
                    record = new ArrayList<>();
                }
            } else if (c == '"') {
                if (field.length() != 0 || closedQuote) {
                    throw new IllegalArgumentException("Unexpected quote in an unquoted CSV field.");
                }
                quoted = true;
            } else if (closedQuote && !Character.isWhitespace(c)) {
                throw new IllegalArgumentException("Unexpected character after a quoted CSV field.");
            } else {
                field.append(c);
            }
        }

        if (quoted) {
            throw new IllegalArgumentException("CSV file ends inside a quoted field.");
        }
        if (field.length() > 0 || !record.isEmpty()) {
            record.add(field.toString());
            records.add(record);
        }
        return records;
    }
}
