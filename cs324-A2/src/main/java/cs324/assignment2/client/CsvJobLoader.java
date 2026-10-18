package cs324.assignment2.client;

import java.io.IOException;
import java.io.Reader;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

final class CsvJobLoader {

    private static final Set<String> JOB_TYPES =
            Set.of("SUM", "PRIMESUM", "MAX", "PRIMECOUNT");
    private static final Set<String> COLUMNS =
            Set.of("job_type", "start", "end", "values");

    private CsvJobLoader() {
    }

    static List<ClientGUI.QueuedJob> load(Path path) throws IOException {
        try (Reader reader = Files.newBufferedReader(path)) {
            return parse(reader);
        }
    }

    static List<ClientGUI.QueuedJob> parse(Reader reader) throws IOException {
        List<List<String>> records = readRecords(reader);
        if (records.isEmpty()) {
            throw new IllegalArgumentException("CSV file is empty.");
        }

        List<String> header = records.get(0);
        if (!header.isEmpty()) {
            header.set(0, header.get(0).replace("\uFEFF", ""));
        }
        Map<String, Integer> columns = new HashMap<>();
        for (int i = 0; i < header.size(); i++) {
            String name = header.get(i).trim().toLowerCase(Locale.ROOT);
            if (!COLUMNS.contains(name)) {
                throw new IllegalArgumentException("Unsupported CSV column: " + name);
            }
            if (columns.put(name, i) != null) {
                throw new IllegalArgumentException("Duplicate CSV column: " + name);
            }
        }
        if (!columns.containsKey("job_type")) {
            throw new IllegalArgumentException("CSV must include a job_type column.");
        }

        List<ClientGUI.QueuedJob> jobs = new ArrayList<>();
        for (int rowIndex = 1; rowIndex < records.size(); rowIndex++) {
            List<String> row = records.get(rowIndex);
            if (row.size() == 1 && row.get(0).isBlank()) {
                continue;
            }
            if (row.size() != header.size()) {
                throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                        + " has " + row.size() + " fields; expected " + header.size() + ".");
            }

            String jobType = value(row, columns, "job_type").toUpperCase(Locale.ROOT);
            if (!JOB_TYPES.contains(jobType)) {
                throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                        + " has unsupported job type: " + jobType);
            }

            String valuesText = value(row, columns, "values");
            String startText = value(row, columns, "start");
            String endText = value(row, columns, "end");
            if (!valuesText.isBlank()) {
                if (!startText.isBlank() || !endText.isBlank()) {
                    throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                            + " must provide either values or start/end, not both.");
                }
                jobs.add(ClientGUI.QueuedJob.values(jobType, parseValues(valuesText, rowIndex + 1)));
            } else {
                if (startText.isBlank() || endText.isBlank()) {
                    throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                            + " must provide values or both start and end.");
                }
                try {
                    int start = Integer.parseInt(startText.trim());
                    int end = Integer.parseInt(endText.trim());
                    if (start > end) {
                        throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                                + " has start greater than end.");
                    }
                    jobs.add(ClientGUI.QueuedJob.range(jobType, start, end));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("CSV row " + (rowIndex + 1)
                            + " has a non-integer start or end.", e);
                }
            }
        }

        if (jobs.isEmpty()) {
            throw new IllegalArgumentException("CSV file contains no job rows.");
        }
        return jobs;
    }

    private static String value(List<String> row, Map<String, Integer> columns, String name) {
        Integer index = columns.get(name);
        return index == null ? "" : row.get(index).trim();
    }

    private static List<Long> parseValues(String text, int rowNumber) {
        String[] tokens = text.trim().split("[;,\\s]+");
        List<Long> values = new ArrayList<>(tokens.length);
        try {
            for (String token : tokens) {
                values.add(Long.parseLong(token));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CSV row " + rowNumber
                    + " has a non-integer value in the values column.", e);
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
