package io.riptidekv;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of an embedded RiptideKV server process.
 *
 * <h2>How it works</h2>
 * <ol>
 *   <li>On {@link #start()}, the platform-specific {@code riptidekv-server}
 *       binary is extracted from the JAR's {@code /native/&lt;os&gt;-&lt;arch&gt;/}
 *       classpath resource to a temporary file.</li>
 *   <li>The binary is launched as a child {@link Process} with the configured
 *       environment variables.</li>
 *   <li>{@link #start()} blocks until the server accepts TCP connections
 *       (up to 10 seconds), then returns.</li>
 *   <li>Any Redis client (Jedis, lettuce, redis-py, redis-cli, go-redis) can
 *       connect to {@code 127.0.0.1:<port>} and issue commands.</li>
 *   <li>{@link #close()} sends SIGTERM and waits for a clean shutdown.</li>
 * </ol>
 *
 * <h2>Quick start — plain Java</h2>
 * <pre>{@code
 * RiptideKVConfig cfg = RiptideKVConfig.builder()
 *     .bind("127.0.0.1:6379")
 *     .dataDir(Paths.get("/var/lib/myapp/rkv"))
 *     .build();
 *
 * try (RiptideKVServer server = new RiptideKVServer(cfg)) {
 *     server.start();
 *     // now talk to it with any Redis client:
 *     try (Jedis j = new Jedis("127.0.0.1", server.getPort())) {
 *         j.set("hello", "world");
 *         System.out.println(j.get("hello")); // world
 *     }
 * } // server shuts down here
 * }</pre>
 *
 * <h2>Quick start — Spring Boot test</h2>
 * <pre>{@code
 * @BeforeAll
 * static void startKv() throws IOException {
 *     server = new RiptideKVServer(
 *         RiptideKVConfig.builder()
 *             .bind("127.0.0.1:16379")
 *             .walSync(false)   // fast for tests
 *             .build());
 *     server.start();
 * }
 *
 * @AfterAll
 * static void stopKv() { server.close(); }
 * }</pre>
 *
 * <h2>Supported platforms</h2>
 * <ul>
 *   <li>Linux x86_64</li>
 *   <li>Linux aarch64 (when included in the release)</li>
 *   <li>macOS x86_64 (Intel)</li>
 *   <li>macOS aarch64 (Apple Silicon)</li>
 *   <li>Windows x86_64</li>
 * </ul>
 */
public final class RiptideKVServer implements AutoCloseable {

    private final RiptideKVConfig config;

    private volatile Process process;
    private volatile Path    extractedBinary;

    /**
     * Create a server manager with the given configuration.
     * The server is not started until {@link #start()} is called.
     */
    public RiptideKVServer(RiptideKVConfig config) {
        if (config == null) throw new IllegalArgumentException("config must not be null");
        this.config = config;
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Extract the native binary, create the data directory, launch the server
     * process, and block until it is accepting TCP connections.
     *
     * @throws IOException           if the binary cannot be extracted, the
     *                               process fails to start, or it does not
     *                               become ready within 10 seconds.
     * @throws IllegalStateException if the server is already running.
     */
    public void start() throws IOException {
        if (process != null && process.isAlive()) {
            throw new IllegalStateException("RiptideKV server is already running (pid=" + process.pid() + ")");
        }

        extractedBinary = extractBinary();

        // Create data directory layout expected by the server.
        Path dataDir = config.getDataDir();
        Path walPath = dataDir.resolve("wal.log");
        Path sstDir  = dataDir.resolve("sst");
        Files.createDirectories(sstDir);

        ProcessBuilder pb = new ProcessBuilder(extractedBinary.toString());
        pb.environment().put("RIPTIDE_BIND",     config.getBind());
        pb.environment().put("RIPTIDE_WAL_PATH", walPath.toString());
        pb.environment().put("RIPTIDE_SST_DIR",  sstDir.toString());
        pb.environment().put("RIPTIDE_FLUSH_KB", String.valueOf(config.getFlushKb()));
        pb.environment().put("RIPTIDE_WAL_SYNC", config.isWalSync() ? "true" : "false");

        // Redirect server stderr/stdout to /dev/null by default.
        // Override by calling pb.inheritIO() before start() if you need logs.
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);

        process = pb.start();

        waitUntilReady(10_000);
    }

    /**
     * Returns the TCP port the server is listening on.
     * Derived from the configured {@code bind} address.
     */
    public int getPort() {
        return config.getPort();
    }

    /**
     * Returns the full bind address string, e.g. {@code "127.0.0.1:6379"}.
     */
    public String getBind() {
        return config.getBind();
    }

    /**
     * Returns {@code true} if the server process is currently running.
     */
    public boolean isRunning() {
        return process != null && process.isAlive();
    }

    /**
     * Terminate the server.
     *
     * <p>Sends SIGTERM (graceful shutdown — flushes the memtable to disk),
     * then waits up to 5 seconds for the process to exit.  If it does not
     * exit in time, {@code SIGKILL} is sent.
     *
     * <p>Safe to call multiple times; subsequent calls are no-ops.
     */
    @Override
    public void close() {
        Process p = process;
        if (p == null || !p.isAlive()) return;

        p.destroy(); // SIGTERM — gives the server a chance to flush
        try {
            if (!p.waitFor(5, TimeUnit.SECONDS)) {
                p.destroyForcibly(); // SIGKILL — no mercy
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            p.destroyForcibly();
        } finally {
            // Delete the extracted temp binary.
            Path bin = extractedBinary;
            if (bin != null) {
                try { Files.deleteIfExists(bin); } catch (IOException ignored) {}
            }
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Detect the current platform, find the matching resource inside the JAR,
     * copy it to a temp file, and mark it executable.
     */
    private Path extractBinary() throws IOException {
        String platform  = detectPlatform();
        boolean isWin    = platform.startsWith("windows");
        String  ext      = isWin ? ".exe" : "";
        String  resource = "/native/" + platform + "/riptidekv-server" + ext;

        InputStream in = RiptideKVServer.class.getResourceAsStream(resource);
        if (in == null) {
            throw new IOException(
                "RiptideKV native binary not bundled for platform '" + platform + "'.\n" +
                "Looked for classpath resource: " + resource + "\n" +
                "Supported platforms: linux-x86_64, linux-aarch64, " +
                "macos-x86_64, macos-aarch64, windows-x86_64.\n" +
                "Make sure you are using the official riptidekv-server JAR from " +
                "GitHub Packages, not a locally-built snapshot without binaries."
            );
        }

        Path tmp = Files.createTempFile("riptidekv-server-", ext.isEmpty() ? "" : ext);
        tmp.toFile().deleteOnExit();

        try (InputStream src = in) {
            Files.copy(src, tmp, StandardCopyOption.REPLACE_EXISTING);
        }

        if (!isWin) {
            // chmod +x so the OS will actually execute it.
            if (!tmp.toFile().setExecutable(true, true)) {
                throw new IOException("Failed to set executable bit on: " + tmp);
            }
        }

        return tmp;
    }

    /**
     * Derive the platform key used as the resource directory name.
     * E.g. {@code "linux-x86_64"}, {@code "macos-aarch64"}, {@code "windows-x86_64"}.
     */
    private static String detectPlatform() {
        String os   = System.getProperty("os.name",  "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch",  "").toLowerCase(Locale.ROOT);

        String osKey;
        if (os.contains("linux")) {
            osKey = "linux";
        } else if (os.contains("mac") || os.contains("darwin")) {
            osKey = "macos";
        } else if (os.contains("windows")) {
            osKey = "windows";
        } else {
            throw new UnsupportedOperationException(
                "Unsupported operating system: '" + System.getProperty("os.name") + "'. " +
                "RiptideKV supports Linux, macOS, and Windows.");
        }

        String archKey;
        if (arch.equals("amd64") || arch.equals("x86_64")) {
            archKey = "x86_64";
        } else if (arch.equals("aarch64") || arch.equals("arm64")) {
            archKey = "aarch64";
        } else {
            throw new UnsupportedOperationException(
                "Unsupported CPU architecture: '" + System.getProperty("os.arch") + "'. " +
                "RiptideKV supports x86_64 (amd64) and aarch64 (arm64).");
        }

        return osKey + "-" + archKey;
    }

    /**
     * Poll the server's TCP port until a connection succeeds or we time out.
     *
     * @param timeoutMs maximum wait time in milliseconds
     * @throws IOException if the process dies early or the timeout expires
     */
    private void waitUntilReady(long timeoutMs) throws IOException {
        // Determine host to probe from the bind address.
        String bindHost = config.getBind();
        int    colon    = bindHost.lastIndexOf(':');
        String host     = bindHost.substring(0, colon);
        int    port     = config.getPort();

        // "0.0.0.0" means "all interfaces" — probe loopback.
        if (host.equals("0.0.0.0") || host.isEmpty()) {
            host = "127.0.0.1";
        }

        long deadline = System.currentTimeMillis() + timeoutMs;
        IOException lastError = null;

        while (System.currentTimeMillis() < deadline) {
            // Check the process hasn't already died.
            if (!process.isAlive()) {
                throw new IOException(
                    "RiptideKV server process exited unexpectedly before becoming ready " +
                    "(exit code: " + process.exitValue() + "). " +
                    "Check that the data directory is writable: " + config.getDataDir());
            }

            try (Socket socket = new Socket(host, port)) {
                return; // TCP handshake succeeded — server is accepting connections
            } catch (IOException e) {
                lastError = e;
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for RiptideKV to start", ie);
                }
            }
        }

        close(); // clean up the zombie process
        throw new IOException(
            "RiptideKV server did not become ready within " + timeoutMs + " ms on " +
            config.getBind() + ". Last connection error: " + lastError);
    }
}
