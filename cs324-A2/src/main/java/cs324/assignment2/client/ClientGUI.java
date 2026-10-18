package cs324.assignment2.client;

import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.WorkerStatusMetric;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ClientGUI extends JFrame {

    private static final String BOOTSTRAP_HOST = "localhost";
    private static final int BOOTSTRAP_PORT = 50051;

    private final JComboBox<String> jobTypeSelector = new JComboBox<>(
            new String[]{"SUM", "PRIMESUM", "MAX", "PRIMECOUNT"});
    private final JTextField startField   = new JTextField("1", 6);
    private final JTextField endField     = new JTextField("50", 6);
    private final JTextArea logArea = new JTextArea(12, 50);
    private final JButton submitButton = new JButton("Submit Job");
    private final JButton loadCsvButton = new JButton("Load CSV");
    private final JButton submitQueueButton = new JButton("Submit Queue");
    private final JButton detailsButton = new JButton("Show worker details");
    private final JPanel detailsPanel = new JPanel(new BorderLayout());
    private final JPanel jobProgressList = new JPanel();
    private final Map<String, List<Object[]>> workerRowsByJob = new HashMap<>();
    private final ExecutorService jobExecutor = Executors.newFixedThreadPool(4);
    private final AtomicInteger nextJobNumber = new AtomicInteger(1);
    private final List<QueuedJob> queuedJobs = new ArrayList<>();
    private final DefaultTableModel queueTableModel = new DefaultTableModel(
            new Object[]{"Job #", "Job Type", "Input", "Status", "Result"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable queueTable = new JTable(queueTableModel);
    private final DefaultTableModel workerTableModel = new DefaultTableModel(
            new Object[]{"Job", "Worker ID", "Status", "Progress", "Active Tasks",
                    "Completed Tasks", "Metrics"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable workerTable = new JTable(workerTableModel);

    public ClientGUI() {
        super("CS324 A2 Client");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.add(new JLabel("Job:"));
        top.add(jobTypeSelector);
        top.add(new JLabel("Start:"));
        top.add(startField);
        top.add(new JLabel("End:"));
        top.add(endField);
        top.add(submitButton);
        top.add(loadCsvButton);
        top.add(submitQueueButton);

        logArea.setEditable(false);
        JScrollPane scroll = new JScrollPane(logArea);

        jobProgressList.setLayout(new BoxLayout(jobProgressList, BoxLayout.Y_AXIS));
        JScrollPane jobProgressScroll = new JScrollPane(jobProgressList);
        jobProgressScroll.setBorder(BorderFactory.createTitledBorder("Job progress"));
        jobProgressScroll.setPreferredSize(new Dimension(750, 170));

        workerTable.setFillsViewportHeight(true);
        detailsPanel.add(new JScrollPane(workerTable), BorderLayout.CENTER);
        detailsPanel.setPreferredSize(new Dimension(600, 170));
        detailsPanel.setVisible(false);

        queueTable.setFillsViewportHeight(true);
        JScrollPane queueScroll = new JScrollPane(queueTable);
        queueScroll.setBorder(BorderFactory.createTitledBorder("CSV submission queue"));
        queueScroll.setPreferredSize(new Dimension(600, 130));

        JSplitPane logAndQueue = new JSplitPane(JSplitPane.VERTICAL_SPLIT, scroll, queueScroll);
        logAndQueue.setResizeWeight(0.65);
        JSplitPane progressAndWork = new JSplitPane(
                JSplitPane.VERTICAL_SPLIT, jobProgressScroll, logAndQueue);
        progressAndWork.setResizeWeight(0.35);
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.add(progressAndWork, BorderLayout.CENTER);
        content.add(detailsPanel, BorderLayout.SOUTH);

        JPanel footer = new JPanel(new BorderLayout());
        footer.add(detailsButton, BorderLayout.WEST);

        add(top, BorderLayout.NORTH);
        add(content, BorderLayout.CENTER);
        add(footer, BorderLayout.SOUTH);

        submitButton.addActionListener(e -> submitJob());
        loadCsvButton.addActionListener(e -> loadCsv());
        submitQueueButton.addActionListener(e -> submitQueuedJobs());
        detailsButton.addActionListener(e -> toggleWorkerDetails());
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent e) {
                jobExecutor.shutdownNow();
            }
        });

        pack();
        setLocationRelativeTo(null);
    }

    private void submitJob() {
        String jobType = (String) jobTypeSelector.getSelectedItem();
        int start, end;
        try {
            start = Integer.parseInt(startField.getText().trim());
            end   = Integer.parseInt(endField.getText().trim());
        } catch (NumberFormatException ex) {
            log("Invalid start/end numbers");
            return;
        }

        if (start > end) {
            log("Start must be less than or equal to end.");
            return;
        }
        startJob(QueuedJob.range(jobType, start, end));
    }

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Load batch jobs from CSV");
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        try {
            List<QueuedJob> loaded = CsvJobLoader.load(chooser.getSelectedFile().toPath());
            for (QueuedJob job : loaded) {
                job.tableRow = queueTableModel.getRowCount();
                queuedJobs.add(job);
                queueTableModel.addRow(new Object[]{job.tableRow + 1, job.jobType,
                        job.inputDescription(), job.status, job.result});
            }
            log("Loaded " + loaded.size() + " job(s) from "
                    + chooser.getSelectedFile().getName() + ".");
        } catch (IOException | IllegalArgumentException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Could not load CSV",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    private void submitQueuedJobs() {
        if (queuedJobs.isEmpty()) {
            log("Load a CSV file before submitting the queue.");
            return;
        }
        List<QueuedJob> pending = queuedJobs.stream()
                .filter(job -> "Queued".equals(job.status))
                .toList();
        if (pending.isEmpty()) {
            log("There are no queued jobs to submit.");
            return;
        }
        pending.forEach(this::startJob);
    }

    private void startJob(QueuedJob job) {
        int jobNumber = nextJobNumber.getAndIncrement();
        JobProgressView progressView = new JobProgressView(
                "Job " + jobNumber + " - " + job.jobType + " (" + job.inputDescription() + ")");
        progressView.progressBar.addMouseListener(new java.awt.event.MouseAdapter() {
            @Override public void mouseClicked(java.awt.event.MouseEvent event) { toggleWorkerDetails(); }
        });
        jobProgressList.add(progressView.panel);
        jobProgressList.revalidate();
        jobProgressList.repaint();
        setQueueStatus(job, "Running", "—");
        log("Job " + jobNumber + " submitted; looking up coordinator.");
        jobExecutor.submit(() -> executeJob(job, jobNumber, progressView));
    }

    private void executeJob(QueuedJob job, int jobNumber, JobProgressView progressView) {
        try {
            JobProgressUpdate result = ResilientJobClient.run(BOOTSTRAP_HOST, BOOTSTRAP_PORT,
                    toRequest(job).toBuilder().setRequestId(java.util.UUID.randomUUID().toString()).build(),
                    update -> showProgress(jobNumber, progressView, update), TimeUnit.MINUTES.toMillis(10));
            finishJob(jobNumber, job, progressView, "Completed", Long.toString(result.getResult()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finishJob(jobNumber, job, progressView, "Cancelled", "Monitoring interrupted");
        } catch (Exception e) {
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            finishJob(jobNumber, job, progressView, "Failed", message);
            publishStatus("Job " + jobNumber + " failed: " + message);
        }
    }

    private static SubmitJobRequest toRequest(QueuedJob job) {
        SubmitJobRequest.Builder request = SubmitJobRequest.newBuilder().setJobType(job.jobType);
        if (job.values.isEmpty()) {
            request.putParameters("start", Integer.toString(job.start))
                    .putParameters("end", Integer.toString(job.end));
        } else {
            request.addAllValues(job.values);
        }
        return request.build();
    }

    private void setQueueStatus(QueuedJob job, String status, String result) {
        job.status = status;
        job.result = result == null || result.isBlank() ? "—" : result;
        if (job.tableRow >= 0) {
            SwingUtilities.invokeLater(() -> {
                queueTableModel.setValueAt(job.status, job.tableRow, 3);
                queueTableModel.setValueAt(job.result, job.tableRow, 4);
            });
        }
    }

    static final class QueuedJob {
        final String jobType;
        final int start;
        final int end;
        final List<Long> values;
        volatile String status = "Queued";
        volatile String result = "—";
        int tableRow = -1;

        private QueuedJob(String jobType, int start, int end, List<Long> values) {
            this.jobType = jobType;
            this.start = start;
            this.end = end;
            this.values = List.copyOf(values);
        }

        static QueuedJob range(String jobType, int start, int end) {
            return new QueuedJob(jobType, start, end, List.of());
        }

        static QueuedJob values(String jobType, List<Long> values) {
            return new QueuedJob(jobType, 0, -1, values);
        }

        String inputDescription() {
            return values.isEmpty() ? start + " .. " + end : values.toString();
        }
    }

    private static final class JobProgressView {
        private final JPanel panel = new JPanel(new BorderLayout(8, 4));
        private final JProgressBar progressBar = new JProgressBar(0, 1000);
        private final JLabel statusLabel = new JLabel("Queued");

        private JobProgressView(String title) {
            panel.setBorder(BorderFactory.createTitledBorder(title));
            progressBar.setStringPainted(true);
            progressBar.setString("0.0%");
            panel.add(progressBar, BorderLayout.CENTER);
            panel.add(statusLabel, BorderLayout.EAST);
            panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 78));
        }
    }

    private void showProgress(int jobNumber, JobProgressView view, JobProgressUpdate update) {
        double percentage = Math.max(0.0, Math.min(100.0, update.getCompletionPercentage()));
        SwingUtilities.invokeLater(() -> {
            view.progressBar.setValue((int) Math.round(percentage * 10.0));
            view.progressBar.setString(String.format("%.1f%%", percentage));
            view.statusLabel.setText(update.getStatus().toString());
        });
        List<Object[]> workerRows = new ArrayList<>();
        for (WorkerStatusMetric worker : update.getWorkersList()) {
            workerRows.add(new Object[]{
                    "Job " + jobNumber,
                    worker.getWorkerId(),
                    worker.getStatus(),
                    String.format("%.1f%%", worker.getCompletionPercentage()),
                    worker.getActiveSubTaskCount(),
                    worker.getCompletedSubTaskCount(),
                    worker.getStatusMessage()
            });
        }
        SwingUtilities.invokeLater(() -> {
            workerRowsByJob.put(update.getJobId(), workerRows);
            refreshWorkerTable();
        });
        publishStatus(String.format("Job %d: %.1f%% - %s - active workers: %d",
                jobNumber, percentage, update.getMessage(), update.getActiveWorkerCount()));
    }

    private void finishJob(int jobNumber, QueuedJob job, JobProgressView view,
                           String status, String result) {
        setQueueStatus(job, status, result);
        SwingUtilities.invokeLater(() -> {
            view.statusLabel.setText(status + " - " + result);
            if ("Completed".equals(status)) {
                view.progressBar.setValue(1000);
                view.progressBar.setString("100.0%");
            }
        });
        publishStatus("Job " + jobNumber + " " + status.toLowerCase() + ": " + result);
    }

    private void refreshWorkerTable() {
        workerTableModel.setRowCount(0);
        for (List<Object[]> rows : workerRowsByJob.values()) {
            for (Object[] row : rows) {
                workerTableModel.addRow(row);
            }
        }
    }

    private void publishStatus(String message) {
        SwingUtilities.invokeLater(() -> log(message));
    }

    private void toggleWorkerDetails() {
        boolean visible = !detailsPanel.isVisible();
        detailsPanel.setVisible(visible);
        detailsButton.setText(visible ? "Hide worker details" : "Show worker details");
        pack();
    }

    private void log(String msg) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(msg + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> new ClientGUI().setVisible(true));
    }
}