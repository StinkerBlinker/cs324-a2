package cs324.assignment2.client;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CsvJobLoaderTest {

    @Test
    void parsesRangesAndExplicitValuesWithoutDroppingDuplicates() throws Exception {
        String csv = "job_type,start,end,values\r\n"
                + "SUM,1,10,\r\n"
                + "MAX,,,\"9, -4, 21, 3, 21\"\r\n"
                + "PRIMECOUNT,,,\"2, 3, 3, 4, 5, 5, 5\"\r\n";

        List<ClientGUI.QueuedJob> jobs = CsvJobLoader.parse(new StringReader(csv));

        assertEquals(3, jobs.size());
        assertEquals("SUM", jobs.get(0).jobType);
        assertEquals(1, jobs.get(0).start);
        assertEquals(10, jobs.get(0).end);
        assertEquals(List.of(9L, -4L, 21L, 3L, 21L), jobs.get(1).values);
        assertEquals(List.of(2L, 3L, 3L, 4L, 5L, 5L, 5L), jobs.get(2).values);
    }

    @Test
    void parsesCommaDelimitedNumberListsInsideQuotedCsvField() throws Exception {
        List<ClientGUI.QueuedJob> jobs = CsvJobLoader.parse(
                new StringReader("job_type,start,end,values\r\nMAX,,,\"1, 2, 3\"\r\n"));
        assertEquals(List.of(1L, 2L, 3L), jobs.get(0).values);
    }

    @Test
    void rejectsInvalidJobRows() {
        String csv = "job_type,start,end,values\nMAX,4,2,\n";
        assertThrows(IllegalArgumentException.class,
                () -> CsvJobLoader.parse(new StringReader(csv)));
    }
}
