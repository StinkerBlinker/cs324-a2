import java.nio.file.*;
import java.util.*;
import java.io.File;
import java.nio.charset.StandardCharsets;

/** Source-mode launcher for Code Runner; application sources stay in Maven modules. */
class RunComponent {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) throw new IllegalArgumentException("Expected component name and project folder");
        String component = args[0];
        boolean build = component.equals("--build") || component.equals("--verify");
        Path root = Path.of(args[1]).toAbsolutePath().normalize();
        String[] target = switch (component) {
            case "BootstrapNode" -> new String[]{"bootstrap-node", "cs324.assignment2.bootstrap.BootstrapNode"};
            case "WorkerNode" -> new String[]{"worker-node", "cs324.assignment2.worker.WorkerNode"};
            case "ClientGUI" -> new String[]{"client", "cs324.assignment2.client.ClientGUI"};
            case "--build", "--verify" -> new String[0];
            default -> throw new IllegalArgumentException("Run BootstrapNode.java, WorkerNode.java, or ClientGUI.java");
        };
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        boolean windows = os.contains("win");
        boolean mac = os.contains("mac");
        if (!windows && !mac) throw new IllegalStateException("This launcher supports Windows and macOS");
        List<Path> homes = new ArrayList<>();
        String configured = System.getenv("JAVA_HOME");
        if (configured != null && !configured.isBlank()) homes.add(Path.of(configured));
        homes.add(Path.of(System.getProperty("java.home")));
        if (windows) {
            String programFiles = System.getenv("ProgramFiles");
            if (programFiles != null) {
                for (String vendor : List.of("Java", "Eclipse Adoptium", "Microsoft", "Amazon Corretto")) {
                    Path directory = Path.of(programFiles, vendor);
                    if (Files.isDirectory(directory)) {
                        try (var installations = Files.list(directory)) {
                            installations.filter(Files::isDirectory).forEach(homes::add);
                        }
                    }
                }
            }
        }
        if (mac) {
            Process lookup = new ProcessBuilder("/usr/libexec/java_home", "-v", "24").redirectError(ProcessBuilder.Redirect.DISCARD).start();
            String home = new String(lookup.getInputStream().readAllBytes()).trim();
            if (lookup.waitFor() == 0 && !home.isBlank()) homes.add(Path.of(home));
        }
        Path java = null;
        for (Path home : homes) {
            Path executable = home.resolve(windows ? "bin/java.exe" : "bin/java");
            Path compiler = home.resolve(windows ? "bin/javac.exe" : "bin/javac");
            if (!Files.isRegularFile(executable) || !Files.isRegularFile(compiler)) continue;
            Process probe = new ProcessBuilder(executable.toString(), "--version").redirectErrorStream(true).start();
            String version = new String(probe.getInputStream().readAllBytes());
            if (probe.waitFor() == 0 && version.matches("(?s)^(?:java|openjdk) 24(?:[. +\\-]).*")) { java = executable; break; }
        }
        if (java == null) throw new IllegalStateException("JDK 24 not found. Set JAVA_HOME to JDK 24 and restart VS Code. Java must also be available on PATH.");
        if (build) {
            List<String> maven = new ArrayList<>(windows
                    ? List.of("cmd.exe", "/d", "/c", "mvnw.cmd", "-B")
                    : List.of("/bin/bash", "./mvnw", "-B"));
            maven.addAll(component.equals("--verify") ? List.of("clean", "verify") : List.of("-DskipTests", "package"));
            Path home = java.getParent().getParent();
            System.out.println("Building with JDK 24: " + home);
            ProcessBuilder builder = new ProcessBuilder(maven).directory(root.toFile()).inheritIO();
            builder.environment().put("JAVA_HOME", home.toString());
            builder.environment().put("PATH", java.getParent() + File.pathSeparator + System.getenv("PATH"));
            System.exit(builder.start().waitFor());
        }
        Path jar = root.resolve(target[0] + "/target/" + target[0] + "-1.0-SNAPSHOT.jar");
        if (!Files.isRegularFile(jar)) throw new IllegalStateException("Build first: Ctrl+Shift+B on Windows or Cmd+Shift+B on macOS. Wait for BUILD SUCCESS.");
        List<String> command;
        if (windows) {
            Path logs = root.resolve(".demo/launch");
            Files.createDirectories(logs);
            Path log = logs.resolve(component + "-" + UUID.randomUUID() + ".log");
            String session = "Set-Location -LiteralPath " + psQuote(root.toString())
                    + "; Write-Host " + psQuote("Starting " + component + " with " + java)
                    + "; & " + psQuote(java.toString()) + " '-cp' " + psQuote(jar.toString()) + " " + psQuote(target[1])
                    + " 2>&1 | Tee-Object -FilePath " + psQuote(log.toString())
                    + "; $componentExit = $LASTEXITCODE; Write-Host ('" + component + " exited with code ' + $componentExit)"
                    + "; Write-Host " + psQuote("Log: " + log);
            String encoded = Base64.getEncoder().encodeToString(session.getBytes(StandardCharsets.UTF_16LE));
            String ps = "$ErrorActionPreference = 'Stop'; Start-Process -FilePath 'powershell.exe'"
                    + " -ArgumentList '-NoProfile', '-NoExit', '-EncodedCommand', " + psQuote(encoded)
                    + " -WorkingDirectory " + psQuote(root.toString());
            command = List.of("powershell.exe", "-NoProfile", "-Command", ps);
        } else {
            String shell = "cd " + shellQuote(root.toString()) + "; " + shellQuote(java.toString())
                    + " -cp " + shellQuote(jar.toString()) + " " + shellQuote(target[1]);
            String apple = "tell application \"Terminal\" to do script \""
                    + shell.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
            command = List.of("osascript", "-e", apple, "-e", "tell application \"Terminal\" to activate");
        }
        if (args.length > 2 && args[2].equals("--check")) {
            System.out.println("Validated " + component + ": JDK 24, component JAR and " + (windows ? "Windows" : "macOS") + " launch command");
            return;
        }
        int result = new ProcessBuilder(command).inheritIO().start().waitFor();
        if (result != 0) throw new IllegalStateException("Component launch failed (exit " + result + ")");
        System.out.println("Opened " + component + " separately. Close its window or press Ctrl+C there to stop it.");
    }
    private static String psQuote(String value) { return "'" + value.replace("'", "''") + "'"; }
    private static String shellQuote(String value) { return "'" + value.replace("'", "'\"'\"'") + "'"; }
}
