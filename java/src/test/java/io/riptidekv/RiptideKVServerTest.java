package io.riptidekv;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RiptideKVServer} lifecycle: start, stop, isRunning, error cases.
 * Each test starts and stops its own server for full isolation.
 */
class RiptideKVServerTest {

    // ── Helpers ───────────────────────────────────────────────────────────────

    static int freePort() throws IOException {
        try (var ss = new ServerSocket(0)) { return ss.getLocalPort(); }
    }

    static RiptideKVServer startServer(int port, Path dataDir) throws IOException {
        var cfg = RiptideKVConfig.builder()
                .bind("127.0.0.1:" + port)
                .dataDir(dataDir)
                .walSync(false)
                .build();
        var server = new RiptideKVServer(cfg);
        server.start();
        return server;
    }

    static boolean canConnect(int port) {
        try (var ignored = new Socket("127.0.0.1", port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // ── Start ─────────────────────────────────────────────────────────────────

    @Test
    void start_serverAcceptsConnections(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp)) {
            assertTrue(server.isRunning());
            assertTrue(canConnect(port), "Should be able to connect after start()");
        }
    }

    @Test
    void start_respondsToRespPing(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp);
             var c = new RespClient(port)) {
            c.send("PING");
            assertEquals("PONG", c.recvSimple());
        }
    }

    @Test
    void start_createsDataDirectoryIfAbsent(@TempDir Path tmp) throws Exception {
        int port = freePort();
        Path nested = tmp.resolve("a").resolve("b").resolve("c");
        // nested does NOT exist yet
        assertFalse(nested.toFile().exists());
        try (var ignored = startServer(port, nested)) {
            assertTrue(nested.toFile().exists(), "start() must create the data directory");
        }
    }

    @Test
    void start_createsSstSubdirectory(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var ignored = startServer(port, tmp)) {
            assertTrue(tmp.resolve("sst").toFile().isDirectory(),
                    "start() must create sst/ subdirectory");
        }
    }

    // ── isRunning ─────────────────────────────────────────────────────────────

    @Test
    void isRunning_falseBeforeStart(@TempDir Path tmp) {
        var cfg = RiptideKVConfig.builder()
                .dataDir(tmp).walSync(false).build();
        var server = new RiptideKVServer(cfg);
        assertFalse(server.isRunning());
    }

    @Test
    void isRunning_trueAfterStart(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp)) {
            assertTrue(server.isRunning());
        }
    }

    @Test
    void isRunning_falseAfterClose(@TempDir Path tmp) throws Exception {
        int port = freePort();
        var server = startServer(port, tmp);
        assertTrue(server.isRunning());
        server.close();
        assertFalse(server.isRunning());
    }

    // ── Stop / close ──────────────────────────────────────────────────────────

    @Test
    void close_releasesPort(@TempDir Path tmp) throws Exception {
        int port = freePort();
        var server = startServer(port, tmp);
        assertTrue(canConnect(port));
        server.close();
        // Give the OS a moment to release the port
        Thread.sleep(200);
        assertFalse(canConnect(port), "Port should be released after close()");
    }

    @Test
    void close_isIdempotent(@TempDir Path tmp) throws Exception {
        int port = freePort();
        var server = startServer(port, tmp);
        server.close();
        assertDoesNotThrow(server::close, "Second close() should not throw");
    }

    @Test
    void tryWithResources_closesAutomatically(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var ignored = startServer(port, tmp)) {
            assertTrue(canConnect(port));
        }
        Thread.sleep(200);
        assertFalse(canConnect(port), "Server should stop at end of try-with-resources");
    }

    // ── Double start ──────────────────────────────────────────────────────────

    @Test
    void start_whenAlreadyRunning_throwsIllegalState(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp)) {
            assertThrows(IllegalStateException.class, server::start,
                    "start() on a running server should throw IllegalStateException");
        }
    }

    // ── getPort / getBind ─────────────────────────────────────────────────────

    @Test
    void getPort_matchesConfig(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp)) {
            assertEquals(port, server.getPort());
        }
    }

    @Test
    void getBind_matchesConfig(@TempDir Path tmp) throws Exception {
        int port = freePort();
        try (var server = startServer(port, tmp)) {
            assertEquals("127.0.0.1:" + port, server.getBind());
        }
    }

    // ── Null config guard ─────────────────────────────────────────────────────

    @Test
    void constructor_nullConfig_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> new RiptideKVServer(null));
    }
}
