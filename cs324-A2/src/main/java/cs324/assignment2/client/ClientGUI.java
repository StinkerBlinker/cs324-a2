package cs324.assignment2.client;

import cs324.assignment2.grpc.*;
import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobServiceGrpc;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.StreamJobProgressRequest;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.SubmitJobResponse;
import cs324.assignment2.job.grpc.WorkerStatusMetric;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

public class ClientGUI extends JFrame {

    private static final String BOOTSTRAP_HOST = "localhost";
    private static final int BOOTSTRAP_PORT = 50051;

    private final JComboBox<String> jobTypeSelector = new JComboBox<>(
            new String[]{"SUM", "PRIMESUM", "MAX", "PRIMECOUNT"});
    private final JTextField startField   = new JTextField("1", 6);
    private final JTextField endField     = new JTextField("50", 6);
    private final JProgressBar progressBar = new JProgressBar(0, 1000);
    private final JTextArea logArea = new JTextArea(12, 50);
    private final JLabel resultLabel = new JLabel("Result: —");
    private final JButton submitButton = new JButton("Submit Job");
    private final JButton loadCsvButton = new JButton("Load CSV");
    private final JButton submitQueueButton = new JButton("Submit Queue");
    private final JButton detailsButton = new JButton("Show worker details");
    private final JPanel detailsPanel = new JPanel(new BorderLayout());
    private final List<QueuedJob> queuedJobs = new ArrayList<>();
    private final DefaultTableModel queueTableModel = new DefaultTableModel(
            new Object[]{"Job Type", "Input", "Status"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable queueTable = new JTable(queueTableModel);
    private final DefaultTableModel workerTableModel = new DefaultTableModel(
            new Object[]{"Worker ID", "Status", "Progress", "Active Tasks",
                    "Completed Tasks", "Metrics"}, 0) {
        @Override
        public boolean isCellEditable(int row, int column) {
            return false;
        }
    };
    private final JTable workerTable = new JTable(workerTableModel);
    private SwingWorker<Void, JobProgressUpdate> activeJob;

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

        progressBar.setStringPainted(true);
        progressBar.setValue(0);
        progressBar.setString("0.0%");

        logArea.setEditable(false);
        JScrollPane scroll = new JScrollPane(logArea);

        JPanel progressPanel = new JPanel(new BorderLayout(8, 4));
        progressPanel.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        progressPanel.add(progressBar, BorderLayout.CENTER);
        progressPanel.add(resultLabel, BorderLayout.EAST);

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
        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.add(progressPanel, BorderLayout.NORTH);
        content.add(logAndQueue, BorderLayout.CENTER);
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

        pack();
        setLocationRelativeTo(null);
    }

    private void submitJob() {
        if (activeJob != null && !activeJob.isDone()) {
            return;
        }

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
        startJobs(List.of(QueuedJob.range(jobType, start, end)), false);
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
                queueTableModel.addRow(new Object[]{job.jobType, job.inputDescription(), job.status});
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
        startJobs(pending, true);
    }

    private void startJobs(List<QueuedJob> jobs, boolean continueAfterFailure) {
        progressBar.setValue(0);
        progressBar.setString("0.0%");
        resultLabel.setText("Result: —");
        workerTableModel.setRowCount(0);
        log("Looking up coordinator via Bootstrap...");

        submitButton.setEnabled(false);
        loadCsvButton.setEnabled(false);
        submitQueueButton.setEnabled(false);
        activeJob = new SwingWorker<>() {
            @Override
            protected Void doInBackground() throws Exception {
                ManagedChannel bootstrapChannel = null;
                ManagedChannel jobChannel = null;
                try {
                    bootstrapChannel = ManagedChannelBuilder
                            .forAddress(BOOTSTRAP_HOST, BOOTSTRAP_PORT)
                            .usePlaintext()
                            .build();
                    List<WorkerInfo> workers = BootstrapServiceGrpc
                            .newBlockingStub(bootstrapChannel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .getActiveWorkers(GetActiveWorkersRequest.newBuilder().build())
                            .getWorkersList();
                    if (workers.isEmpty()) {
                        throw new IllegalStateException("No active workers found.");
                    }

                    WorkerInfo coordinator = findCoordinator(workers);
                    jobChannel = ManagedChannelBuilder
                            .forAddress(coordinator.getHost(), coordinator.getPort())
                            .usePlaintext()
                            .build();
                    JobServiceGrpc.JobServiceBlockingStub jobStub =
                            JobServiceGrpc.newBlockingStub(jobChannel)
                                    .withDeadlineAfter(10, TimeUnit.SECONDS);
                    for (QueuedJob job : jobs) {
                        setQueueStatus(job, "Running");
                        try {
                            SubmitJobResponse accepted = jobStub.submitJob(toRequest(job));
                            publishStatus("Job " + accepted.getJobId() + " accepted: "
                                    + accepted.getStatus());

                            JobServiceGrpc.JobServiceBlockingStub progressStub =
                                    JobServiceGrpc.newBlockingStub(jobChannel)
                                            .withDeadlineAfter(10, TimeUnit.MINUTES);
                            var updates = progressStub.streamJobProgress(
                                    StreamJobProgressRequest.newBuilder()
                                            .setJobId(accepted.getJobId())
                                            .build());
                            JobStatus terminalStatus = JobStatus.JOB_STATUS_UNSPECIFIED;
                            while (updates.hasNext()) {
                                JobProgressUpdate update = updates.next();
                                terminalStatus = update.getStatus();
                                publish(update);
                            }
                            setQueueStatus(job, terminalStatus == JobStatus.JOB_STATUS_COMPLETED
                                    ? "Completed" : "Failed");
                        } catch (Exception e) {
                            setQueueStatus(job, "Failed");
                            publishStatus("Job " + job.jobType + " failed: " + e.getMessage());
                            if (!continueAfterFailure) {
                                throw e;
                            }
                        }
                    }
                    return null;
                } finally {
                    shutdownChannel(jobChannel);
                    shutdownChannel(bootstrapChannel);
                }
            }

            @Override
            protected void process(List<JobProgressUpdate> updates) {
                for (JobProgressUpdate update : updates) {
                    showProgress(update);
                }
            }

            @Override
            protected void done() {
                submitButton.setEnabled(true);
                loadCsvButton.setEnabled(true);
                submitQueueButton.setEnabled(true);
                try {
                    get();
                    log("Job processing finished.");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log("Job monitoring was interrupted.");
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    log("Job monitoring failed: "
                            + (cause == null ? e.getMessage() : cause.getMessage()));
                }
            }
        };
        activeJob.execute();
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

    private void setQueueStatus(QueuedJob job, String status) {
        job.status = status;
        if (job.tableRow >= 0) {
            SwingUtilities.invokeLater(() -> queueTableModel.setValueAt(
                    status, job.tableRow, 2));
        }
    }

    static final class QueuedJob {
        final String jobType;
        final int start;
        final int end;
        final List<Long> values;
        volatile String status = "Queued";
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

    private WorkerInfo findCoordinator(List<WorkerInfo> workers) {
        for (WorkerInfo worker : workers) {
            ManagedChannel channel = ManagedChannelBuilder
                    .forAddress(worker.getHost(), worker.getPort())
                    .usePlaintext()
                    .build();
            try {
                int coordinatorId = WorkerServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(2, TimeUnit.SECONDS)
                        .getCoordinatorId(GetCoordinatorIdRequest.newBuilder().build())
                        .getCoordinatorId();
                for (WorkerInfo candidate : workers) {
                    if (candidate.getId() == coordinatorId) {
                        log("Coordinator is Worker " + coordinatorId);
                        return candidate;
                    }
                }
            } catch (StatusRuntimeException e) {
                log("Could not query Worker " + worker.getId()
                        + " for coordinator status: " + e.getStatus().getDescription());
            } finally {
                shutdownChannel(channel);
            }
        }

        WorkerInfo fallback = workers.get(0);
        log("No coordinator announced; trying Worker " + fallback.getId());
        return fallback;
    }

    private void showProgress(JobProgressUpdate update) {
        double percentage = Math.max(0.0, Math.min(100.0, update.getCompletionPercentage()));
        progressBar.setValue((int) Math.round(percentage * 10.0));
        progressBar.setString(String.format("%.1f%%", percentage));
        workerTableModel.setRowCount(0);
        for (WorkerStatusMetric worker : update.getWorkersList()) {
            workerTableModel.addRow(new Object[]{
                    worker.getWorkerId(),
                    worker.getStatus(),
                    String.format("%.1f%%", worker.getCompletionPercentage()),
                    worker.getActiveSubTaskCount(),
                    worker.getCompletedSubTaskCount(),
                    worker.getStatusMessage()
            });
        }

        if (update.getStatus() == JobStatus.JOB_STATUS_COMPLETED) {
            resultLabel.setText("Result: " + update.getResult());
        } else if (update.getStatus() == JobStatus.JOB_STATUS_FAILED) {
            resultLabel.setText("Job failed");
        } else if (update.getStatus() == JobStatus.JOB_STATUS_CANCELLED) {
            resultLabel.setText("Job cancelled");
        }
        log(String.format("Progress %.1f%% - %s - active workers: %d",
                percentage, update.getMessage(), update.getActiveWorkerCount()));
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

    private static void shutdownChannel(ManagedChannel channel) {
        if (channel != null) {
            channel.shutdown();
            try {
                if (!channel.awaitTermination(1, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
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