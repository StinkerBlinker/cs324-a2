package cs324.assignment2.client;

import cs324.assignment2.job.grpc.JobProgressUpdate;
import cs324.assignment2.job.grpc.JobStatus;
import cs324.assignment2.job.grpc.SubmitJobRequest;
import cs324.assignment2.job.grpc.WorkerStatusMetric;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileNameExtensionFilter;
import java.awt.*;
import java.io.IOException;
import java.io.File;
import java.nio.file.Path;
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
    private final JTextField numbersField = new JTextField("9, -4, 21, 3, 21", 24);
    private final JTextArea logArea = new JTextArea(12, 50);
    private final JButton submitButton = new JButton("Submit Job");
    private final JButton loadCsvButton = new JButton("Load CSV");
    private final JButton submitQueueButton = new JButton("Submit Queue");
    private final JButton detailsButton = new JButton("Show worker details");
    private final JPanel detailsPanel = new JPanel(new BorderLayout());
    private final JPanel jobProgressList = new JPanel();
    private final JSplitPane progressAndLogs = new JSplitPane(JSplitPane.VERTICAL_SPLIT);
    private final Map<Integer, List<Object[]>> workerRowsByJob = new HashMap<>();
    private static final Color PAGE = new Color(245, 247, 250);
    private static final Color INK = new Color(30, 41, 59);
    private static final Color MUTED = new Color(94, 108, 128);
    private static final Color ACCENT = new Color(37, 99, 185);
    private final JPanel numbersPanel = new JPanel(new BorderLayout(10, 0));
    private final JLabel detailsHeading = new JLabel("Worker breakdown - All jobs");
    private final JLabel queueSummary = new JLabel("No CSV jobs loaded");
    private final JLabel emptyProgress = new JLabel("Submit a job or load a CSV to get started.");
    private int selectedJobNumber = -1;
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
        super("DistriLab - Job Console");
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        JPanel root = new JPanel(new BorderLayout(0, 16));
        root.setBorder(new EmptyBorder(20, 24, 16, 24));
        root.setBackground(PAGE);
        setContentPane(root);

        JPanel heading = new JPanel(new BorderLayout(12, 4));
        heading.setOpaque(false);
        JLabel title = new JLabel("Distributed jobs");
        title.setFont(title.getFont().deriveFont(Font.BOLD, 23f));
        title.setForeground(INK);
        heading.add(title, BorderLayout.NORTH);
        JLabel subtitle = new JLabel("Submit work, follow live progress, and inspect each worker.");
        subtitle.setForeground(MUTED);
        heading.add(subtitle, BorderLayout.SOUTH);

        JPanel inputs = new JPanel(new GridBagLayout());
        inputs.setOpaque(false);
        GridBagConstraints c = new GridBagConstraints();
        c.gridy = 0;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.WEST;
        c.insets = new Insets(0, 0, 0, 8);
        addInput(inputs, new JLabel("Job type"), c, 0, 0);
        jobTypeSelector.setPreferredSize(new Dimension(130, 34));
        addInput(inputs, jobTypeSelector, c, 1, 0.25);
        addInput(inputs, new JLabel("Start"), c, 2, 0);
        startField.setColumns(5);
        addInput(inputs, startField, c, 3, 0.15);
        addInput(inputs, new JLabel("End"), c, 4, 0);
        endField.setColumns(5);
        addInput(inputs, endField, c, 5, 0.15);
        styleButton(submitButton, "Submit the current inputs as a new job");
        addInput(inputs, submitButton, c, 6, 0);
        // A native system look and feel can ignore button backgrounds. This
        // explicit surface keeps the CSV action distinct on macOS and Windows.
        loadCsvButton.setBackground(ACCENT);
        loadCsvButton.setForeground(Color.WHITE);
        loadCsvButton.setOpaque(true);
        loadCsvButton.setUI(new javax.swing.plaf.basic.BasicButtonUI());
        loadCsvButton.setContentAreaFilled(true);
        loadCsvButton.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(ACCENT), new EmptyBorder(8, 15, 8, 15)));
        loadCsvButton.setFocusPainted(false);
        loadCsvButton.setToolTipText("Choose a .csv file to add jobs to the batch queue");
        addInput(inputs, loadCsvButton, c, 7, 0);
        styleButton(submitQueueButton, "Submit all queued CSV jobs; up to four are monitored concurrently");
        c.insets = new Insets(0, 0, 0, 0);
        addInput(inputs, submitQueueButton, c, 8, 0);

        numbersPanel.setOpaque(false);
        numbersPanel.add(new JLabel("Numbers"), BorderLayout.WEST);
        numbersPanel.add(numbersField, BorderLayout.CENTER);
        numbersField.setToolTipText("Comma-separated integers; negative numbers and duplicates are supported");
        startField.setToolTipText("First integer in the inclusive range");
        endField.setToolTipText("Last integer in the inclusive range");
        JPanel inputCard = new JPanel(new BorderLayout(0, 12));
        inputCard.setBackground(Color.WHITE);
        inputCard.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(222, 228, 236)), new EmptyBorder(14, 14, 14, 14)));
        inputCard.add(inputs, BorderLayout.NORTH);
        inputCard.add(numbersPanel, BorderLayout.SOUTH);
        JPanel top = new JPanel(new BorderLayout(0, 16));
        top.setOpaque(false);
        top.add(heading, BorderLayout.NORTH);
        top.add(inputCard, BorderLayout.CENTER);
        root.add(top, BorderLayout.NORTH);

        jobProgressList.setLayout(new BoxLayout(jobProgressList, BoxLayout.Y_AXIS));
        jobProgressList.setBackground(Color.WHITE);
        jobProgressList.setBorder(new EmptyBorder(12, 12, 4, 12));
        emptyProgress.setForeground(MUTED);
        emptyProgress.setBorder(new EmptyBorder(14, 4, 14, 4));
        jobProgressList.add(emptyProgress);
        JScrollPane progressScroll = new JScrollPane(jobProgressList);
        progressScroll.setBorder(BorderFactory.createEmptyBorder());
        progressScroll.getVerticalScrollBar().setUnitIncrement(18);
        JPanel progressPanel = section("Job progress", progressScroll);
        JLabel progressHint = new JLabel("Click a progress bar for worker details");
        progressHint.setForeground(MUTED);
        progressHint.setBorder(new EmptyBorder(0, 12, 10, 12));
        progressPanel.add(progressHint, BorderLayout.SOUTH);

        logArea.setEditable(false);
        logArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        logArea.setMargin(new Insets(10, 12, 10, 12));
        logArea.setLineWrap(true);
        logArea.setWrapStyleWord(true);
        logArea.setBackground(new Color(249, 250, 252));
        logArea.setForeground(INK);
        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setBorder(BorderFactory.createEmptyBorder());
        styleTable(queueTable);
        queueTable.setAutoCreateRowSorter(true);
        queueTable.getColumnModel().getColumn(2).setPreferredWidth(340);
        JScrollPane queueScroll = new JScrollPane(queueTable);
        queueScroll.setBorder(BorderFactory.createEmptyBorder());
        JPanel queuePanel = new JPanel(new BorderLayout());
        queueSummary.setForeground(MUTED);
        queueSummary.setBorder(new EmptyBorder(8, 12, 8, 12));
        queuePanel.add(queueSummary, BorderLayout.NORTH);
        queuePanel.add(queueScroll, BorderLayout.CENTER);
        JTabbedPane activityTabs = new JTabbedPane();
        activityTabs.addTab("Live logs", logScroll);
        activityTabs.addTab("CSV queue", queuePanel);
        activityTabs.setToolTipTextAt(1, "Inspect loaded inputs, submission status and results");
        JSplitPane progressAndLogs = new JSplitPane(JSplitPane.VERTICAL_SPLIT,
                progressPanel, activityTabs);
        progressAndLogs.setResizeWeight(0.52);
        progressAndLogs.setContinuousLayout(true);
        progressAndLogs.setBorder(BorderFactory.createEmptyBorder());
        progressAndLogs.setDividerSize(8);
        progressPanel.setMinimumSize(new Dimension(0, 85));
        activityTabs.setMinimumSize(new Dimension(0, 85));

        styleTable(workerTable);
        workerTable.getColumnModel().getColumn(6).setPreferredWidth(220);
        JScrollPane workerScroll = new JScrollPane(workerTable);
        workerScroll.setBorder(BorderFactory.createEmptyBorder());
        JPanel detailsHeader = new JPanel(new BorderLayout(12, 0));
        detailsHeader.setOpaque(false);
        detailsHeading.setFont(detailsHeading.getFont().deriveFont(Font.BOLD));
        detailsHeader.add(detailsHeading, BorderLayout.CENTER);
        JButton allJobs = new JButton("All jobs");
        allJobs.setToolTipText("Show worker metrics for every submitted job");
        allJobs.addActionListener(e -> { selectedJobNumber = -1; refreshWorkerTable(); });
        detailsHeader.add(allJobs, BorderLayout.EAST);
        detailsPanel.setBackground(Color.WHITE);
        detailsPanel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createLineBorder(new Color(222, 228, 236)), new EmptyBorder(10, 12, 10, 12)));
        detailsPanel.add(detailsHeader, BorderLayout.NORTH);
        detailsPanel.add(workerScroll, BorderLayout.CENTER);
        detailsPanel.setPreferredSize(new Dimension(0, 140));
        detailsPanel.setVisible(false);
        JPanel content = new JPanel(new BorderLayout(0, 12));
        content.setOpaque(false);
        content.add(progressAndLogs, BorderLayout.CENTER);
        content.add(detailsPanel, BorderLayout.SOUTH);
        root.add(content, BorderLayout.CENTER);

        JPanel footer = new JPanel(new BorderLayout(12, 0));
        footer.setOpaque(false);
        styleButton(detailsButton, "Show or hide worker-level job progress");
        footer.add(detailsButton, BorderLayout.WEST);
        JLabel footerHint = new JLabel("Live progress reflects completed distributed work");
        footerHint.setForeground(MUTED);
        footer.add(footerHint, BorderLayout.EAST);
        root.add(footer, BorderLayout.SOUTH);
        jobTypeSelector.addActionListener(e -> updateInputMode());
        updateInputMode();
        submitButton.addActionListener(e -> submitJob());
        loadCsvButton.addActionListener(e -> loadCsv());
        submitQueueButton.addActionListener(e -> submitQueuedJobs());
        detailsButton.addActionListener(e -> toggleWorkerDetails());
        getRootPane().setDefaultButton(submitButton);
        addWindowListener(new java.awt.event.WindowAdapter() {
            @Override public void windowClosing(java.awt.event.WindowEvent e) {
                jobExecutor.shutdownNow();
            }
        });
        setMinimumSize(new Dimension(960, 740));
        setSize(1100, 760);
        setLocationRelativeTo(null);
        SwingUtilities.invokeLater(() -> progressAndLogs.setDividerLocation(0.52));
    }

    private static void addInput(JPanel panel, JComponent component,
                                 GridBagConstraints constraints, int column, double weight) {
        constraints.gridx = column;
        constraints.weightx = weight;
        panel.add(component, constraints);
    }

    private static void styleButton(JButton button, String tooltip) {
        button.setMargin(new Insets(8, 12, 8, 12));
        button.setToolTipText(tooltip);
    }

    private static JPanel section(String title, JComponent content) {
        JPanel panel = new JPanel(new BorderLayout(0, 4));
        panel.setBackground(Color.WHITE);
        panel.setBorder(BorderFactory.createLineBorder(new Color(222, 228, 236)));
        JLabel label = new JLabel(title);
        label.setFont(label.getFont().deriveFont(Font.BOLD, 14f));
        label.setForeground(INK);
        label.setBorder(new EmptyBorder(12, 12, 4, 12));
        panel.add(label, BorderLayout.NORTH);
        panel.add(content, BorderLayout.CENTER);
        return panel;
    }

    private static void styleTable(JTable table) {
        table.setFillsViewportHeight(true);
        table.setRowHeight(28);
        table.setShowVerticalLines(false);
        table.setGridColor(new Color(234, 238, 244));
        table.setIntercellSpacing(new Dimension(0, 1));
        table.setSelectionBackground(new Color(222, 234, 252));
        table.setSelectionForeground(INK);
        table.getTableHeader().setReorderingAllowed(false);
        table.getTableHeader().setFont(table.getTableHeader().getFont().deriveFont(Font.BOLD));
        table.setPreferredScrollableViewportSize(new Dimension(0, 110));
    }

    private void updateInputMode() {
        String type = (String) jobTypeSelector.getSelectedItem();
        boolean list = "MAX".equals(type) || "PRIMECOUNT".equals(type);
        startField.setEnabled(!list);
        endField.setEnabled(!list);
        numbersField.setEnabled(list);
        numbersPanel.setVisible(list);
        revalidate();
        repaint();
    }

    private void submitJob() {
        String jobType = (String) jobTypeSelector.getSelectedItem();
        if ("MAX".equals(jobType) || "PRIMECOUNT".equals(jobType)) {
            try { startJob(QueuedJob.values(jobType, CsvJobLoader.parseManualValues(numbersField.getText()))); }
            catch (IllegalArgumentException error) { log(error.getMessage()); }
            return;
        }
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
        File directory = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().toFile();
        if (!directory.isDirectory() || directory.toPath().getParent() == null) {
            directory = new File(System.getProperty("user.home", "."));
        }
        JFileChooser chooser = new JFileChooser(directory);
        chooser.setCurrentDirectory(directory);
        chooser.setDialogTitle("Load batch jobs from CSV");
        chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        chooser.setAcceptAllFileFilterUsed(false);
        chooser.setFileFilter(new FileNameExtensionFilter("CSV files (*.csv)", "csv"));
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) return;
        File selected = chooser.getSelectedFile();
        if (!selected.getName().toLowerCase(java.util.Locale.ROOT).endsWith(".csv")) {
            JOptionPane.showMessageDialog(this, "Select a file with the .csv extension.",
                    "CSV files only", JOptionPane.WARNING_MESSAGE);
            return;
        }
        loadCsvButton.setEnabled(false);
        loadCsvButton.setText("Loading...");
        jobExecutor.submit(() -> {
            try {
                List<QueuedJob> loaded = CsvJobLoader.load(selected.toPath());
                SwingUtilities.invokeLater(() -> {
                    for (QueuedJob job : loaded) {
                        job.tableRow = queueTableModel.getRowCount();
                        queuedJobs.add(job);
                        queueTableModel.addRow(new Object[]{job.tableRow + 1, job.jobType,
                                job.inputDescription(), job.status, job.result});
                    }
                    queueSummary.setText(queuedJobs.size() + " CSV job(s) loaded - Submit Queue to run pending jobs");
                    log("Loaded " + loaded.size() + " job(s) from " + selected.getName() + ".");
                });
            } catch (IOException | IllegalArgumentException error) {
                SwingUtilities.invokeLater(() -> JOptionPane.showMessageDialog(this,
                        error.getMessage(), "Could not load CSV", JOptionPane.ERROR_MESSAGE));
            } finally {
                SwingUtilities.invokeLater(() -> {
                    loadCsvButton.setEnabled(true);
                    loadCsvButton.setText("Load CSV");
                });
            }
        });
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
            @Override public void mouseClicked(java.awt.event.MouseEvent event) {
                selectedJobNumber = jobNumber;
                refreshWorkerTable();
                showWorkerDetails(true);
            }
        });
        jobProgressList.remove(emptyProgress);
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
                    update -> showProgress(jobNumber, job, progressView, update), TimeUnit.MINUTES.toMillis(10));
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
        private final JPanel panel = new JPanel(new BorderLayout(0, 8));
        private final JProgressBar progressBar = new JProgressBar(0, 1000);
        private final JLabel statusLabel = new JLabel("Queued");
        private final JLabel resultLabel = new JLabel("Waiting for the coordinator");

        private JobProgressView(String title) {
            panel.setBackground(Color.WHITE);
            panel.setBorder(new EmptyBorder(8, 4, 16, 4));
            JPanel heading = new JPanel(new BorderLayout(12, 0));
            heading.setOpaque(false);
            JLabel titleLabel = new JLabel(title);
            titleLabel.setFont(titleLabel.getFont().deriveFont(Font.BOLD));
            titleLabel.setToolTipText(title);
            titleLabel.setMinimumSize(new Dimension(0, 18));
            heading.add(titleLabel, BorderLayout.CENTER);
            statusLabel.setForeground(ACCENT);
            heading.add(statusLabel, BorderLayout.EAST);
            progressBar.setStringPainted(true);
            progressBar.setString("0.0%");
            progressBar.setPreferredSize(new Dimension(0, 24));
            progressBar.setForeground(ACCENT);
            progressBar.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
            progressBar.setToolTipText("Click to view this job's worker breakdown");
            resultLabel.setForeground(MUTED);
            panel.add(heading, BorderLayout.NORTH);
            panel.add(progressBar, BorderLayout.CENTER);
            panel.add(resultLabel, BorderLayout.SOUTH);
            panel.setMaximumSize(new Dimension(Integer.MAX_VALUE, 108));
            panel.setAlignmentX(Component.LEFT_ALIGNMENT);
        }
    }

    private void showProgress(int jobNumber, QueuedJob job, JobProgressView view,
                              JobProgressUpdate update) {
        double percentage = Math.max(0.0, Math.min(100.0, update.getCompletionPercentage()));
        String status = displayJobStatus(update.getStatus());
        setQueueStatus(job, status, job.result);
        SwingUtilities.invokeLater(() -> {
            view.progressBar.setValue((int) Math.round(percentage * 10.0));
            view.progressBar.setString(String.format("%.1f%%", percentage));
            view.statusLabel.setText(status);
            view.resultLabel.setText(update.getMessage().isBlank() ? "Receiving worker progress" : update.getMessage());
            view.resultLabel.setToolTipText(update.getMessage());
        });
        List<Object[]> workerRows = new ArrayList<>();
        for (WorkerStatusMetric worker : update.getWorkersList()) {
            workerRows.add(new Object[]{
                    "Job " + jobNumber,
                    worker.getWorkerId(),
                    displayWorkerStatus(worker.getStatus()),
                    String.format("%.1f%%", worker.getCompletionPercentage()),
                    worker.getActiveSubTaskCount(),
                    worker.getCompletedSubTaskCount(),
                    worker.getStatusMessage()
            });
        }
        SwingUtilities.invokeLater(() -> {
            workerRowsByJob.put(jobNumber, workerRows);
            refreshWorkerTable();
        });
        publishStatus(String.format("Job %d: %.1f%% - %s - active workers: %d",
                jobNumber, percentage, status + " - " + update.getMessage(),
                update.getActiveWorkerCount()));
    }

    private static String displayJobStatus(JobStatus status) {
        return switch (status) {
            case JOB_STATUS_QUEUED -> "Queued";
            case JOB_STATUS_RUNNING -> "Active";
            case JOB_STATUS_SLOW -> "Slow";
            case JOB_STATUS_COMPLETED -> "Completed";
            case JOB_STATUS_FAILED -> "Failed";
            case JOB_STATUS_CANCELLED -> "Cancelled";
            default -> "Unknown";
        };
    }

    private static String displayWorkerStatus(cs324.assignment2.job.grpc.WorkerStatus status) {
        return switch (status) {
            case WORKER_STATUS_IDLE -> "Idle";
            case WORKER_STATUS_ACTIVE -> "Active";
            case WORKER_STATUS_SLOW -> "Slow";
            case WORKER_STATUS_UNAVAILABLE -> "Unavailable";
            case WORKER_STATUS_FAILED -> "Failed";
            default -> "Unknown";
        };
    }

    private void finishJob(int jobNumber, QueuedJob job, JobProgressView view,
                           String status, String result) {
        setQueueStatus(job, status, result);
        SwingUtilities.invokeLater(() -> {
            view.statusLabel.setText(status);
            view.statusLabel.setForeground("Completed".equals(status) ? new Color(24, 122, 81)
                    : "Failed".equals(status) ? new Color(185, 48, 48) : MUTED);
            view.resultLabel.setText("Completed".equals(status) ? "Result: " + result : result);
            view.resultLabel.setToolTipText(result);
            if ("Completed".equals(status)) {
                view.progressBar.setValue(1000);
                view.progressBar.setString("100.0%");
            }
        });
        publishStatus("Job " + jobNumber + " " + status.toLowerCase() + ": " + result);
    }

    private void refreshWorkerTable() {
        workerTableModel.setRowCount(0);
        detailsHeading.setText("Worker breakdown - "
                + (selectedJobNumber < 0 ? "All jobs" : "Job " + selectedJobNumber));
        workerRowsByJob.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .filter(entry -> selectedJobNumber < 0 || entry.getKey() == selectedJobNumber)
                .forEach(entry -> entry.getValue().forEach(workerTableModel::addRow));
    }

    private void publishStatus(String message) {
        SwingUtilities.invokeLater(() -> log(message));
    }

    private void toggleWorkerDetails() {
        showWorkerDetails(!detailsPanel.isVisible());
    }

    private void showWorkerDetails(boolean visible) {
        detailsPanel.setVisible(visible);
        detailsButton.setText(visible ? "Hide worker details" : "Show worker details");
        revalidate();
        repaint();
        SwingUtilities.invokeLater(() -> progressAndLogs.setDividerLocation(0.52));
    }

    private void log(String msg) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(msg + "\n");
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
            } catch (ReflectiveOperationException | UnsupportedLookAndFeelException ignored) {
                // Keep the default Swing look and feel when a native theme is unavailable.
            }
            new ClientGUI().setVisible(true);
        });
    }
}