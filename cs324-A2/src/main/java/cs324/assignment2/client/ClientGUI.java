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
    private final JButton detailsButton = new JButton("Show worker details");
    private final JPanel detailsPanel = new JPanel(new BorderLayout());
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

        JPanel content = new JPanel(new BorderLayout(8, 8));
        content.add(progressPanel, BorderLayout.NORTH);
        content.add(scroll, BorderLayout.CENTER);
        content.add(detailsPanel, BorderLayout.SOUTH);

        JPanel footer = new JPanel(new BorderLayout());
        footer.add(detailsButton, BorderLayout.WEST);

        add(top, BorderLayout.NORTH);
        add(content, BorderLayout.CENTER);
        add(footer, BorderLayout.SOUTH);

        submitButton.addActionListener(e -> submitJob());
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

        progressBar.setValue(0);
        progressBar.setString("0.0%");
        resultLabel.setText("Result: —");
        workerTableModel.setRowCount(0);
        log("Looking up coordinator via Bootstrap...");

        submitButton.setEnabled(false);
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
                    JobServiceGrpc.JobServiceBlockingStub jobStub = JobServiceGrpc
                            .newBlockingStub(jobChannel)
                            .withDeadlineAfter(10, TimeUnit.SECONDS);
                    SubmitJobResponse accepted = jobStub.submitJob(
                            SubmitJobRequest.newBuilder()
                                    .setJobType(jobType)
                                    .putParameters("start", Integer.toString(start))
                                    .putParameters("end", Integer.toString(end))
                                    .build());
                    publishStatus("Job " + accepted.getJobId() + " accepted: "
                            + accepted.getStatus());

                    JobServiceGrpc.JobServiceBlockingStub progressStub = JobServiceGrpc
                            .newBlockingStub(jobChannel)
                            .withDeadlineAfter(10, TimeUnit.MINUTES);
                    var updates = progressStub.streamJobProgress(
                            StreamJobProgressRequest.newBuilder()
                                    .setJobId(accepted.getJobId())
                                    .build());
                    while (updates.hasNext()) {
                        publish(updates.next());
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
                try {
                    get();
                    log("Job progress stream ended.");
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