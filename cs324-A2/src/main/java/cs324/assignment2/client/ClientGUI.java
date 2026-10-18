package cs324.assignment2.client;

import cs324.assignment2.grpc.*;
import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobServiceGrpc;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.StreamJobProgressRequest;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.SubmitJobResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import javax.swing.*;
import java.awt.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class ClientGUI extends JFrame {

    private static final String BOOTSTRAP_HOST = "localhost";
    private static final int BOOTSTRAP_PORT = 50051;

    private final JTextField jobTypeField = new JTextField("PRIMESUM", 10);
    private final JTextField startField   = new JTextField("1", 6);
    private final JTextField endField     = new JTextField("50", 6);
    private final JProgressBar progressBar = new JProgressBar(0, 100);
    private final JTextArea logArea = new JTextArea(12, 50);
    private final JLabel resultLabel = new JLabel("Result: —");

    public ClientGUI() {
        super("CS324 A2 Client");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setLayout(new BorderLayout(8, 8));

        JPanel top = new JPanel(new FlowLayout(FlowLayout.LEFT));
        top.add(new JLabel("Job:"));
        top.add(jobTypeField);
        top.add(new JLabel("Start:"));
        top.add(startField);
        top.add(new JLabel("End:"));
        top.add(endField);
        JButton submitBtn = new JButton("Submit Job");
        top.add(submitBtn);

        progressBar.setStringPainted(true);
        progressBar.setValue(0);

        logArea.setEditable(false);
        JScrollPane scroll = new JScrollPane(logArea);

        add(top, BorderLayout.NORTH);
        add(progressBar, BorderLayout.CENTER);
        add(scroll, BorderLayout.SOUTH);
        add(resultLabel, BorderLayout.EAST);

        submitBtn.addActionListener(e -> submitJob());

        pack();
        setLocationRelativeTo(null);
    }

    private void submitJob() {
        String jobType = jobTypeField.getText().trim().toUpperCase();
        int start, end;
        try {
            start = Integer.parseInt(startField.getText().trim());
            end   = Integer.parseInt(endField.getText().trim());
        } catch (NumberFormatException ex) {
            log("Invalid start/end numbers");
            return;
        }

        progressBar.setValue(0);
        resultLabel.setText("Result: —");
        log("Looking up coordinator via Bootstrap...");

        new Thread(() -> {
            try {
                ManagedChannel bootstrapCh = ManagedChannelBuilder
                        .forAddress(BOOTSTRAP_HOST, BOOTSTRAP_PORT)
                        .usePlaintext()
                        .build();
                BootstrapServiceGrpc.BootstrapServiceBlockingStub bootstrap =
                        BootstrapServiceGrpc.newBlockingStub(bootstrapCh);

                List<WorkerInfo> workers = bootstrap.getActiveWorkers(
                        GetActiveWorkersRequest.newBuilder().build()).getWorkersList();
                bootstrapCh.shutdown();

                if (workers.isEmpty()) {
                    log("No active workers found.");
                    return;
                }

                int coordinatorId = -1;
                WorkerInfo coordinatorInfo = null;
                for (WorkerInfo w : workers) {
                    try {
                        ManagedChannel ch = ManagedChannelBuilder
                                .forAddress(w.getHost(), w.getPort())
                                .usePlaintext()
                                .build();
                        WorkerServiceGrpc.WorkerServiceBlockingStub stub =
                                WorkerServiceGrpc.newBlockingStub(ch);
                        int cid = stub.getCoordinatorId(
                                GetCoordinatorIdRequest.newBuilder().build()).getCoordinatorId();
                        ch.shutdown();
                        if (cid != -1) {
                            coordinatorId = cid;
                            for (WorkerInfo cand : workers) {
                                if (cand.getId() == cid) {
                                    coordinatorInfo = cand;
                                    break;
                                }
                            }
                            break;
                        }
                    } catch (Exception ignored) {}
                }

                if (coordinatorInfo == null) {
                    coordinatorInfo = workers.get(0);
                    log("No coordinator announced yet — sending to Worker "
                            + coordinatorInfo.getId());
                } else {
                    log("Coordinator is Worker " + coordinatorId);
                }

                ManagedChannel jobCh = ManagedChannelBuilder
                        .forAddress(coordinatorInfo.getHost(), coordinatorInfo.getPort())
                        .usePlaintext()
                        .build();
                JobServiceGrpc.JobServiceStub asyncStub = JobServiceGrpc.newStub(jobCh);

                CountDownLatch latch = new CountDownLatch(1);

                asyncStub.submitJob(
                        SubmitJobRequest.newBuilder()
                                .setJobType(jobType)
                                .putParameters("start", Integer.toString(start))
                                .putParameters("end", Integer.toString(end))
                                .build(),
                        new StreamObserver<SubmitJobResponse>() {
                            @Override
                            public void onNext(SubmitJobResponse response) {
                                log("Job " + response.getJobId() + " accepted: "
                                        + response.getStatus());
                                asyncStub.streamJobProgress(
                                        StreamJobProgressRequest.newBuilder()
                                                .setJobId(response.getJobId())
                                                .build(),
                                        new StreamObserver<JobProgressUpdate>() {
                                            @Override
                                            public void onNext(JobProgressUpdate update) {
                                                SwingUtilities.invokeLater(() -> {
                                                    progressBar.setValue((int) update
                                                            .getCompletionPercentage());
                                                    if (update.getStatus()
                                                            == JobStatus.JOB_STATUS_COMPLETED) {
                                                        resultLabel.setText("Result: "
                                                                + update.getResult());
                                                    } else if (update.getStatus()
                                                            == JobStatus.JOB_STATUS_FAILED) {
                                                        resultLabel.setText("Job failed");
                                                    }

                                                    StringBuilder sb = new StringBuilder(
                                                            String.format("Progress: %.0f%%, active workers: %d",
                                                                    update.getCompletionPercentage(),
                                                                    update.getActiveWorkerCount()));
                                                    for (var worker : update.getWorkersList()) {
                                                        sb.append(" [W").append(worker.getWorkerId())
                                                          .append(" ").append(String.format("%.0f",
                                                                  worker.getCompletionPercentage()))
                                                          .append("% ").append(worker.getStatus()).append("]");
                                                    }
                                                    log(sb.toString());
                                                });
                                            }

                                            @Override
                                            public void onError(Throwable t) {
                                                log("Progress stream failed: " + t.getMessage());
                                                latch.countDown();
                                            }

                                            @Override
                                            public void onCompleted() {
                                                log("Job stream completed.");
                                                latch.countDown();
                                            }
                                        });
                            }

                            @Override
                            public void onError(Throwable t) {
                                log("Job submission failed: " + t.getMessage());
                                latch.countDown();
                            }

                            @Override
                            public void onCompleted() {
                                // The progress stream completes the job lifecycle.
                            }
                        });

                latch.await(120, TimeUnit.SECONDS);
                jobCh.shutdown();

            } catch (Exception e) {
                log("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }, "client-submit").start();
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