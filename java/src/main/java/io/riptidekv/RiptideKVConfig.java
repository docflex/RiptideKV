package io.riptidekv;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Immutable configuration for a {@link RiptideKVServer} instance.
 *
 * <p>Use the fluent {@link Builder} to construct a config:
 *
 * <pre>{@code
 * RiptideKVConfig config = RiptideKVConfig.builder()
 *     .bind("127.0.0.1:6380")          // TCP address to listen on
 *     .dataDir(Paths.get("/tmp/rkv"))   // where WAL + SSTables live
 *     .flushKb(4096)                    // flush memtable at 4 MiB
 *     .walSync(false)                   // disable fsync for speed in tests
 *     .build();
 * }</pre>
 *
 * <p>All fields map directly to environment variables consumed by
 * {@code riptidekv-server}:
 *
 * <table border="1">
 *   <tr><th>Builder method</th><th>Env variable</th><th>Default</th></tr>
 *   <tr><td>{@link Builder#bind}</td><td>RIPTIDE_BIND</td><td>127.0.0.1:6379</td></tr>
 *   <tr><td>{@link Builder#dataDir} (WAL)</td><td>RIPTIDE_WAL_PATH</td><td>&lt;dataDir&gt;/wal.log</td></tr>
 *   <tr><td>{@link Builder#dataDir} (SST)</td><td>RIPTIDE_SST_DIR</td><td>&lt;dataDir&gt;/sst</td></tr>
 *   <tr><td>{@link Builder#flushKb}</td><td>RIPTIDE_FLUSH_KB</td><td>1024</td></tr>
 *   <tr><td>{@link Builder#walSync}</td><td>RIPTIDE_WAL_SYNC</td><td>true</td></tr>
 * </table>
 */
public final class RiptideKVConfig {

    private final String bind;
    private final Path   dataDir;
    private final int    flushKb;
    private final boolean walSync;

    private RiptideKVConfig(Builder b) {
        this.bind    = b.bind;
        this.dataDir = b.dataDir;
        this.flushKb = b.flushKb;
        this.walSync = b.walSync;
    }

    /** TCP address the server binds to, e.g. {@code "127.0.0.1:6379"}. */
    public String getBind()    { return bind; }

    /** Directory under which {@code wal.log} and {@code sst/} are stored. */
    public Path   getDataDir() { return dataDir; }

    /** Memtable flush threshold in KiB. */
    public int    getFlushKb() { return flushKb; }

    /** Whether the server calls {@code fsync} after every WAL write. */
    public boolean isWalSync() { return walSync; }

    /**
     * Extracts the port number from the bind address.
     * E.g. {@code "127.0.0.1:6380"} → {@code 6380}.
     */
    public int getPort() {
        int colon = bind.lastIndexOf(':');
        if (colon < 0) throw new IllegalStateException("Bind address has no port: " + bind);
        return Integer.parseInt(bind.substring(colon + 1));
    }

    /** Returns a new {@link Builder} with all defaults set. */
    public static Builder builder() { return new Builder(); }

    // ── Builder ──────────────────────────────────────────────────────────────

    /** Fluent builder for {@link RiptideKVConfig}. */
    public static final class Builder {

        private String bind    = "127.0.0.1:6379";
        private Path   dataDir = Paths.get(System.getProperty("java.io.tmpdir"), "riptidekv");
        private int    flushKb = 1024;
        private boolean walSync = true;

        private Builder() {}

        /**
         * TCP address the server binds to.
         * Use {@code "127.0.0.1:0"} to let the OS pick a free port (useful for
         * tests, but the actual port must then be read from the process output).
         * Default: {@code "127.0.0.1:6379"}.
         */
        public Builder bind(String bind) {
            if (bind == null || bind.isBlank()) throw new IllegalArgumentException("bind must not be blank");
            this.bind = bind;
            return this;
        }

        /**
         * Root directory where the WAL file ({@code wal.log}) and SSTable
         * directory ({@code sst/}) will be created.  The directory is created
         * automatically by {@link RiptideKVServer#start()} if it does not exist.
         * Default: {@code <java.io.tmpdir>/riptidekv}.
         */
        public Builder dataDir(Path dataDir) {
            if (dataDir == null) throw new IllegalArgumentException("dataDir must not be null");
            this.dataDir = dataDir;
            return this;
        }

        /**
         * Memtable flush threshold in KiB.  When the in-memory write buffer
         * reaches this size, its contents are flushed to a new immutable
         * SSTable on disk.  Larger values mean fewer flushes and more RAM
         * usage; smaller values mean more frequent disk writes.
         * Default: {@code 1024} (= 1 MiB).
         */
        public Builder flushKb(int flushKb) {
            if (flushKb <= 0) throw new IllegalArgumentException("flushKb must be > 0");
            this.flushKb = flushKb;
            return this;
        }

        /**
         * Whether to call {@code fsync} after every WAL write.
         * {@code true} (default) — fully durable; every acknowledged write
         *   survives a power loss or OS crash.
         * {@code false} — up to ~1 second of writes may be lost on a hard
         *   crash, but throughput is significantly higher.  Safe for
         *   ephemeral/test data.
         */
        public Builder walSync(boolean walSync) {
            this.walSync = walSync;
            return this;
        }

        /** Build the immutable {@link RiptideKVConfig}. */
        public RiptideKVConfig build() {
            int colon = bind.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException(
                        "bind must be in host:port format (no colon found): " + bind);
            }
            try {
                int port = Integer.parseInt(bind.substring(colon + 1));
                if (port < 0 || port > 65535) {
                    throw new IllegalArgumentException(
                            "bind port must be in [0, 65535]: " + port);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "bind port is not a valid integer: " + bind, e);
            }
            return new RiptideKVConfig(this);
        }
    }
}
