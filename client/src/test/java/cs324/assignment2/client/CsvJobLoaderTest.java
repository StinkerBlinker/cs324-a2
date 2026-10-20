package cs324.assignment2.client;

import org.junit.jupiter.api.Test;
import java.io.StringReader;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class CsvJobLoaderTest {
    @Test void manualListsRetainOrderNegativeValuesAndDuplicates() {
        assertEquals(List.of(-9L, 3L, 3L, 2L), CsvJobLoader.parseManualValues("-9, 3; 3 2"));
        assertThrows(IllegalArgumentException.class, () -> CsvJobLoader.parseManualValues(" "));
        assertThrows(IllegalArgumentException.class, () -> CsvJobLoader.parseManualValues("2, nope, 3"));
    }

    @Test void allRowsFormOneDatasetAndRetainDuplicates() throws Exception {
        assertEquals(List.of(9L, -4L, 21L, 3L, 21L, 2L),
                CsvJobLoader.parseValues(new StringReader("9,-4,21\r\n3,21,2\r\n")));
    }

    @Test void acceptsQuotedNumbersWhitespaceAndByteOrderMark() throws Exception {
        assertEquals(List.of(2L, 3L, -5L), CsvJobLoader.parseValues(
                new StringReader("\uFEFF2,\"3\", -5\n")));
    }

    @Test void rejectsEmptyCellsInsteadOfSilentlyDroppingValues() {
        for (String text : List.of("", "\n\n", "1,,3", "1,2,", "1,nope,3")) {
            assertThrows(IllegalArgumentException.class,
                    () -> CsvJobLoader.parseValues(new StringReader(text)));
        }
    }

    @Test void rejectsJobDefinitionHeaders() {
        assertThrows(IllegalArgumentException.class, () -> CsvJobLoader.parseValues(
                new StringReader("job_type,start,end,values\nMAX,,,1\n")));
    }

    @Test void largeDatasetIsNotSplitIntoSeparateJobsOrTruncated() throws Exception {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < 100_000; i++) csv.append(i == 0 ? "" : i % 100 == 0 ? "\n" : ",").append(i);
        List<Long> values = CsvJobLoader.parseValues(new StringReader(csv.toString()));
        assertEquals(100_000, values.size());
        assertEquals(0L, values.getFirst());
        assertEquals(99_999L, values.getLast());
        assertEquals(4_999_950_000L, values.stream().mapToLong(Long::longValue).sum());
    }
}
