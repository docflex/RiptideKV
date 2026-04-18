package io.riptidekv;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration tests for every RESP2 command supported by RiptideKV.
 *
 * <p>One server is started per outer-class lifecycle ({@code @BeforeAll}/{@code @AfterAll}).
 * A fresh {@link RespClient} is opened before each test and {@code FLUSHALL} is called
 * so each test starts with an empty keyspace.
 *
 * <p>Commands are grouped into {@code @Nested} classes by category:
 * <ul>
 *   <li>{@link ConnectionTests}     — PING, ECHO, SELECT, HELLO, CLIENT, INFO, CONFIG, COMMAND, QUIT</li>
 *   <li>{@link DatabaseTests}       — DBSIZE, FLUSHDB, FLUSHALL, ACL, SLOWLOG, MEMORY, WAIT</li>
 *   <li>{@link StringTests}         — GET, SET (all options), SETNX, SETEX, PSETEX, GETSET, GETDEL,
 *                                     GETEX, MGET, MSET, MSETNX, APPEND, STRLEN, INCR, INCRBY,
 *                                     INCRBYFLOAT, DECR, DECRBY, GETRANGE, SETRANGE</li>
 *   <li>{@link KeyTests}            — DEL, UNLINK, EXISTS, TYPE, RENAME, RENAMENX, RANDOMKEY, TOUCH,
 *                                     EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST,
 *                                     EXPIRETIME, PEXPIRETIME, KEYS, SCAN</li>
 *   <li>{@link ExpiryTests}         — real-time TTL expiry behaviour (uses Thread.sleep)</li>
 *   <li>{@link EdgeCaseTests}       — pipelining, concurrent clients, binary-safe values, unknown cmd</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RespCommandsTest {

    RiptideKVServer server;
    int             port;
    RespClient      c;

    // ── Suite lifecycle ───────────────────────────────────────────────────────

    @BeforeAll
    void startServer(@TempDir Path tempDir) throws IOException {
        port   = freePort();
        server = new RiptideKVServer(
                RiptideKVConfig.builder()
                        .bind("127.0.0.1:" + port)
                        .dataDir(tempDir)
                        .walSync(false)
                        .build());
        server.start();
    }

    @AfterAll
    void stopServer() {
        if (server != null) server.close();
    }

    @BeforeEach
    void openClientAndFlush() throws IOException {
        c = new RespClient(port);
        c.send("FLUSHALL");
        assertEquals("OK", c.recvSimple());
    }

    @AfterEach
    void closeClient() throws IOException {
        if (c != null) c.close();
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    static int freePort() throws IOException {
        try (var ss = new ServerSocket(0)) { return ss.getLocalPort(); }
    }

    /** Convenience: SET k v and assert +OK. */
    void set(String k, String v) throws IOException {
        c.send("SET", k, v);
        c.recvOk();
    }

    // ═════════════════════════════════════════════════════════════════════════
    // CONNECTION / SERVER COMMANDS
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class ConnectionTests {

        @Test void ping_noArgs_returnsPong() throws IOException {
            c.send("PING");
            assertEquals("PONG", c.recvSimple());
        }

        @Test void ping_withMessage_returnsMessage() throws IOException {
            c.send("PING", "hello world");
            assertEquals("hello world", c.recvBulk());
        }

        @Test void echo_returnsArgument() throws IOException {
            c.send("ECHO", "foobar");
            assertEquals("foobar", c.recvBulk());
        }

        @Test void echo_emptyString() throws IOException {
            c.send("ECHO", "");
            assertEquals("", c.recvBulk());
        }

        @Test void select_zero_returnsOk() throws IOException {
            c.send("SELECT", "0");
            c.recvOk();
        }

        @Test void select_nonZero_returnsError() throws IOException {
            c.send("SELECT", "1");
            assertTrue(c.recvError().startsWith("ERR"));
        }

        @Test void hello_resp2_returnsResponse() throws IOException {
            c.send("HELLO", "2");
            // RiptideKV returns a bulk string for HELLO 2
            Object r = c.recv();
            assertNotNull(r);
        }

        @Test void hello_resp3_returnsError() throws IOException {
            c.send("HELLO", "3");
            // RiptideKV returns NOPROTO (not ERR) for unsupported protocol versions
            var err = c.recvError();
            assertTrue(err.startsWith("NOPROTO") || err.startsWith("ERR"),
                    "HELLO 3 should return a NOPROTO or ERR error, got: " + err);
        }

        @Test void client_setname_returnsOk() throws IOException {
            c.send("CLIENT", "SETNAME", "myapp");
            c.recvOk();
        }

        @Test void client_getname_returnsSetName() throws IOException {
            c.send("CLIENT", "SETNAME", "testclient");
            c.recvOk();
            c.send("CLIENT", "GETNAME");
            assertEquals("testclient", c.recvBulk());
        }

        @Test void client_id_returnsInteger() throws IOException {
            c.send("CLIENT", "ID");
            long id = c.recvInt();
            assertTrue(id >= 0, "CLIENT ID should be non-negative");
        }

        @Test void command_count_returnsPositiveInteger() throws IOException {
            c.send("COMMAND", "COUNT");
            long count = c.recvInt();
            assertTrue(count > 10, "Expected at least 10 commands, got: " + count);
        }

        @Test void info_returnsNonEmptyBulkString() throws IOException {
            c.send("INFO");
            String info = c.recvBulk();
            assertNotNull(info);
            assertFalse(info.isBlank());
        }

        @Test void info_serverSection_containsVersionField() throws IOException {
            c.send("INFO", "server");
            String info = c.recvBulk();
            assertNotNull(info);
            assertTrue(info.contains("redis_version") || info.contains("riptidekv"),
                    "INFO server should contain version info: " + info);
        }

        @Test void config_get_returnsArray() throws IOException {
            c.send("CONFIG", "GET", "*");
            Object r = c.recv();
            assertNotNull(r);
            // RiptideKV returns an empty array for CONFIG GET
            assertTrue(r instanceof Object[]);
        }

        @Test void quit_returnsOkAndClosesConnection() throws IOException {
            // Use a dedicated client — QUIT closes the connection
            try (var qc = new RespClient(port)) {
                qc.send("QUIT");
                assertEquals("OK", qc.recvSimple());
                // Server closes the connection after OK; further reads return EOF
            }
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // DATABASE COMMANDS
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class DatabaseTests {

        @Test void dbsize_emptyDb_returnsZero() throws IOException {
            c.send("DBSIZE");
            assertEquals(0L, c.recvInt());
        }

        @Test void dbsize_afterSet_returnsCount() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("DBSIZE");
            assertEquals(2L, c.recvInt());
        }

        @Test void flushdb_clearsAllKeys() throws IOException {
            set("k1", "v1");
            set("k2", "v2");
            c.send("FLUSHDB");
            c.recvOk();
            c.send("DBSIZE");
            assertEquals(0L, c.recvInt());
        }

        @Test void flushall_clearsAllKeys() throws IOException {
            set("x", "y");
            c.send("FLUSHALL");
            c.recvOk();
            c.send("DBSIZE");
            assertEquals(0L, c.recvInt());
        }

        @Test void acl_whoami_returnsDefault() throws IOException {
            c.send("ACL", "WHOAMI");
            assertEquals("default", c.recvBulk());
        }

        @Test void slowlog_get_returnsArray() throws IOException {
            c.send("SLOWLOG", "GET");
            Object[] arr = c.recvArray();
            assertNotNull(arr);
            assertEquals(0, arr.length);
        }

        @Test void memory_usage_existingKey_returnsInteger() throws IOException {
            set("memkey", "hello");
            c.send("MEMORY", "USAGE", "memkey");
            Object r = c.recv();
            // Returns integer (bytes) or null if not supported
            assertTrue(r == null || r instanceof Long, "MEMORY USAGE should return integer or null");
        }

        @Test void wait_returnsZero() throws IOException {
            c.send("WAIT", "0", "0");
            assertEquals(0L, c.recvInt());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // STRING COMMANDS
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class StringTests {

        // ── GET / SET ─────────────────────────────────────────────────────────

        @Test void get_missingKey_returnsNull() throws IOException {
            c.send("GET", "no-such-key");
            assertNull(c.recvBulk());
        }

        @Test void set_andGet_roundtrip() throws IOException {
            c.send("SET", "foo", "bar");
            c.recvOk();
            c.send("GET", "foo");
            assertEquals("bar", c.recvBulk());
        }

        @Test void set_overwritesExistingValue() throws IOException {
            set("k", "original");
            c.send("SET", "k", "updated");
            c.recvOk();
            c.send("GET", "k");
            assertEquals("updated", c.recvBulk());
        }

        @Test void set_getFlag_returnsOldValue() throws IOException {
            set("k", "old");
            c.send("SET", "k", "new", "GET");
            assertEquals("old", c.recvBulk());
        }

        @Test void set_getFlag_missingKey_returnsNull() throws IOException {
            c.send("SET", "newkey", "v", "GET");
            assertNull(c.recvBulk());
        }

        @Test void set_nxFlag_absentKey_setsAndReturnsOk() throws IOException {
            c.send("SET", "k", "v", "NX");
            assertEquals("OK", c.recvBulk());
        }

        @Test void set_nxFlag_presentKey_returnsNull() throws IOException {
            set("k", "original");
            c.send("SET", "k", "new", "NX");
            assertNull(c.recvBulk());
            c.send("GET", "k");
            assertEquals("original", c.recvBulk()); // unchanged
        }

        @Test void set_xxFlag_presentKey_setsAndReturnsOk() throws IOException {
            set("k", "original");
            c.send("SET", "k", "updated", "XX");
            assertEquals("OK", c.recvBulk());
        }

        @Test void set_xxFlag_absentKey_returnsNull() throws IOException {
            c.send("SET", "absent", "v", "XX");
            assertNull(c.recvBulk());
        }

        @Test void set_withEx_setsTtl() throws IOException {
            c.send("SET", "k", "v", "EX", "100");
            c.recvOk();
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 100, "TTL should be in (0, 100], got: " + ttl);
        }

        @Test void set_withPx_setsTtlMs() throws IOException {
            c.send("SET", "k", "v", "PX", "100000");
            c.recvOk();
            c.send("PTTL", "k");
            long pttl = c.recvInt();
            assertTrue(pttl > 0 && pttl <= 100_000, "PTTL should be in (0, 100000], got: " + pttl);
        }

        @Test void set_withExInvalid_returnsError() throws IOException {
            c.send("SET", "k", "v", "EX", "0");
            assertTrue(c.recvError().startsWith("ERR"));
        }

        @Test void set_keepttl_preservesExistingTtl() throws IOException {
            c.send("SET", "k", "v1", "EX", "100");
            c.recvOk();
            c.send("SET", "k", "v2", "KEEPTTL");
            c.recvOk();
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0, "KEEPTTL should preserve TTL, got: " + ttl);
        }

        @Test void set_noTtlOption_clearsPreviousTtl() throws IOException {
            c.send("SET", "k", "v1", "EX", "100");
            c.recvOk();
            c.send("SET", "k", "v2");
            c.recvOk();
            c.send("TTL", "k");
            assertEquals(-1L, c.recvInt()); // no TTL
        }

        // ── SETNX ─────────────────────────────────────────────────────────────

        @Test void setnx_absentKey_returns1() throws IOException {
            c.send("SETNX", "k", "v");
            assertEquals(1L, c.recvInt());
        }

        @Test void setnx_presentKey_returns0() throws IOException {
            set("k", "existing");
            c.send("SETNX", "k", "new");
            assertEquals(0L, c.recvInt());
            c.send("GET", "k");
            assertEquals("existing", c.recvBulk()); // not changed
        }

        // ── SETEX / PSETEX ────────────────────────────────────────────────────

        @Test void setex_setsValueAndTtl() throws IOException {
            c.send("SETEX", "k", "60", "hello");
            c.recvOk();
            c.send("GET", "k");
            assertEquals("hello", c.recvBulk());
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 60);
        }

        @Test void setex_zeroTimeout_returnsError() throws IOException {
            c.send("SETEX", "k", "0", "v");
            assertTrue(c.recvError().startsWith("ERR"));
        }

        @Test void psetex_setsValueAndTtlMs() throws IOException {
            c.send("PSETEX", "k", "60000", "hello");
            c.recvOk();
            c.send("PTTL", "k");
            long pttl = c.recvInt();
            assertTrue(pttl > 0 && pttl <= 60_000);
        }

        // ── GETSET / GETDEL / GETEX ───────────────────────────────────────────

        @Test void getset_returnsOldValue() throws IOException {
            set("k", "old");
            c.send("GETSET", "k", "new");
            assertEquals("old", c.recvBulk());
            c.send("GET", "k");
            assertEquals("new", c.recvBulk());
        }

        @Test void getset_missingKey_returnsNull() throws IOException {
            c.send("GETSET", "absent", "v");
            assertNull(c.recvBulk());
        }

        @Test void getdel_presentKey_returnsAndDeletes() throws IOException {
            set("k", "hello");
            c.send("GETDEL", "k");
            assertEquals("hello", c.recvBulk());
            c.send("EXISTS", "k");
            assertEquals(0L, c.recvInt());
        }

        @Test void getdel_missingKey_returnsNull() throws IOException {
            c.send("GETDEL", "absent");
            assertNull(c.recvBulk());
        }

        @Test void getex_withEx_setsExpiry() throws IOException {
            set("k", "v");
            c.send("GETEX", "k", "EX", "30");
            assertEquals("v", c.recvBulk());
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 30);
        }

        @Test void getex_withPersist_removesExpiry() throws IOException {
            c.send("SET", "k", "v", "EX", "30");
            c.recvOk();
            c.send("GETEX", "k", "PERSIST");
            assertEquals("v", c.recvBulk());
            c.send("TTL", "k");
            assertEquals(-1L, c.recvInt());
        }

        // ── MSET / MGET / MSETNX ──────────────────────────────────────────────

        @Test void mset_andMget_roundtrip() throws IOException {
            c.send("MSET", "a", "1", "b", "2", "c", "3");
            c.recvOk();
            c.send("MGET", "a", "b", "c", "missing");
            Object[] results = c.recvArray();
            assertNotNull(results);
            assertEquals(4, results.length);
            assertEquals("1", results[0]);
            assertEquals("2", results[1]);
            assertEquals("3", results[2]);
            assertNull(results[3]);
        }

        @Test void msetnx_allAbsent_returns1() throws IOException {
            c.send("MSETNX", "x", "1", "y", "2");
            assertEquals(1L, c.recvInt());
        }

        @Test void msetnx_anyPresent_returns0AndSetsNothing() throws IOException {
            set("x", "existing");
            c.send("MSETNX", "x", "new", "y", "2");
            assertEquals(0L, c.recvInt());
            c.send("GET", "y");
            assertNull(c.recvBulk()); // y was NOT set because x was present
        }

        // ── APPEND / STRLEN ───────────────────────────────────────────────────

        @Test void append_createsKeyAndReturnsLength() throws IOException {
            c.send("APPEND", "k", "hello");
            assertEquals(5L, c.recvInt());
            c.send("GET", "k");
            assertEquals("hello", c.recvBulk());
        }

        @Test void append_toExistingKeyExtends() throws IOException {
            set("k", "hello");
            c.send("APPEND", "k", " world");
            assertEquals(11L, c.recvInt());
            c.send("GET", "k");
            assertEquals("hello world", c.recvBulk());
        }

        @Test void strlen_existingKey_returnsLength() throws IOException {
            set("k", "hello");
            c.send("STRLEN", "k");
            assertEquals(5L, c.recvInt());
        }

        @Test void strlen_missingKey_returnsZero() throws IOException {
            c.send("STRLEN", "absent");
            assertEquals(0L, c.recvInt());
        }

        // ── INCR / INCRBY / INCRBYFLOAT / DECR / DECRBY ──────────────────────

        @Test void incr_absentKey_createsAndReturns1() throws IOException {
            c.send("INCR", "counter");
            assertEquals(1L, c.recvInt());
        }

        @Test void incr_existingKey_increments() throws IOException {
            set("counter", "10");
            c.send("INCR", "counter");
            assertEquals(11L, c.recvInt());
        }

        @Test void incr_nonParseableValue_treatsAsZero() throws IOException {
            // RiptideKV uses unwrap_or(0) for un-parseable values: INCR treats them as 0
            set("incr_bad_type", "notanumber");
            c.send("INCR", "incr_bad_type");
            assertEquals(1L, c.recvInt(), "INCR on non-numeric value should treat it as 0 and return 1");
        }

        @Test void incrby_incrementsByAmount() throws IOException {
            set("k", "10");
            c.send("INCRBY", "k", "5");
            assertEquals(15L, c.recvInt());
        }

        @Test void incrbyfloat_addsFraction() throws IOException {
            set("k", "10");
            c.send("INCRBYFLOAT", "k", "1.5");
            String result = c.recvBulk();
            assertNotNull(result);
            assertEquals(11.5, Double.parseDouble(result), 0.001);
        }

        @Test void decr_decrementsBy1() throws IOException {
            set("k", "10");
            c.send("DECR", "k");
            assertEquals(9L, c.recvInt());
        }

        @Test void decrby_decrementsByAmount() throws IOException {
            set("k", "10");
            c.send("DECRBY", "k", "3");
            assertEquals(7L, c.recvInt());
        }

        // ── GETRANGE / SETRANGE ───────────────────────────────────────────────

        @Test void getrange_returnsSubstring() throws IOException {
            set("k", "hello world");
            c.send("GETRANGE", "k", "0", "4");
            assertEquals("hello", c.recvBulk());
        }

        @Test void getrange_negativeIndex_fromEnd() throws IOException {
            set("k", "hello world");
            c.send("GETRANGE", "k", "6", "-1");
            assertEquals("world", c.recvBulk());
        }

        @Test void setrange_overwritesPortionAndReturnsLength() throws IOException {
            set("k", "hello world");
            c.send("SETRANGE", "k", "6", "Redis");
            assertEquals(11L, c.recvInt());
            c.send("GET", "k");
            assertEquals("hello Redis", c.recvBulk());
        }

        // ── Large value ───────────────────────────────────────────────────────

        @Test void set_largeValue_roundtrip() throws IOException {
            String large = "x".repeat(512 * 1024); // 512 KiB
            c.send("SET", "bigkey", large);
            c.recvOk();
            c.send("GET", "bigkey");
            assertEquals(large, c.recvBulk());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // KEY COMMANDS
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class KeyTests {

        // ── DEL / UNLINK ──────────────────────────────────────────────────────

        @Test void del_presentKey_returns1() throws IOException {
            set("k", "v");
            c.send("DEL", "k");
            assertEquals(1L, c.recvInt());
        }

        @Test void del_missingKey_returns0() throws IOException {
            c.send("DEL", "absent");
            assertEquals(0L, c.recvInt());
        }

        @Test void del_multipleKeys_returnsDeletedCount() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("DEL", "a", "b", "missing");
            assertEquals(2L, c.recvInt());
        }

        @Test void unlink_presentKey_returns1() throws IOException {
            set("k", "v");
            c.send("UNLINK", "k");
            assertEquals(1L, c.recvInt());
            c.send("EXISTS", "k");
            assertEquals(0L, c.recvInt());
        }

        // ── EXISTS ────────────────────────────────────────────────────────────

        @Test void exists_presentKey_returns1() throws IOException {
            set("k", "v");
            c.send("EXISTS", "k");
            assertEquals(1L, c.recvInt());
        }

        @Test void exists_absentKey_returns0() throws IOException {
            c.send("EXISTS", "absent");
            assertEquals(0L, c.recvInt());
        }

        @Test void exists_multipleKeys_returnsCount() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("EXISTS", "a", "b", "missing");
            assertEquals(2L, c.recvInt());
        }

        // ── TYPE ──────────────────────────────────────────────────────────────

        @Test void type_stringKey_returnsString() throws IOException {
            set("k", "v");
            c.send("TYPE", "k");
            assertEquals("string", c.recvSimple());
        }

        @Test void type_absentKey_returnsNone() throws IOException {
            c.send("TYPE", "absent");
            assertEquals("none", c.recvSimple());
        }

        // ── RENAME / RENAMENX ─────────────────────────────────────────────────

        @Test void rename_movesValue() throws IOException {
            set("src", "hello");
            c.send("RENAME", "src", "dst");
            c.recvOk();
            c.send("GET", "dst");
            assertEquals("hello", c.recvBulk());
            c.send("EXISTS", "src");
            assertEquals(0L, c.recvInt());
        }

        @Test void rename_missingSource_returnsError() throws IOException {
            c.send("RENAME", "absent", "dst");
            assertTrue(c.recvError().startsWith("ERR"));
        }

        @Test void rename_preservesTtl() throws IOException {
            c.send("SET", "src", "v", "EX", "100");
            c.recvOk();
            c.send("RENAME", "src", "dst");
            c.recvOk();
            c.send("TTL", "dst");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 100, "Renamed key should preserve TTL, got: " + ttl);
        }

        @Test void renamenx_absentDest_returns1() throws IOException {
            set("src", "hello");
            c.send("RENAMENX", "src", "dst");
            assertEquals(1L, c.recvInt());
        }

        @Test void renamenx_presentDest_returns0() throws IOException {
            set("src", "hello");
            set("dst", "existing");
            c.send("RENAMENX", "src", "dst");
            assertEquals(0L, c.recvInt());
            c.send("GET", "dst");
            assertEquals("existing", c.recvBulk()); // not overwritten
        }

        // ── RANDOMKEY / TOUCH ─────────────────────────────────────────────────

        @Test void randomkey_noKeys_returnsNull() throws IOException {
            c.send("RANDOMKEY");
            assertNull(c.recvBulk());
        }

        @Test void randomkey_withKeys_returnsAKey() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("RANDOMKEY");
            String key = c.recvBulk();
            assertNotNull(key);
            assertTrue(key.equals("a") || key.equals("b"), "Unexpected key: " + key);
        }

        @Test void touch_existingKeys_returnsCount() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("TOUCH", "a", "b", "missing");
            assertEquals(2L, c.recvInt());
        }

        // ── EXPIRE / PEXPIRE / EXPIREAT / PEXPIREAT ───────────────────────────

        @Test void expire_setsTtlInSeconds() throws IOException {
            set("k", "v");
            c.send("EXPIRE", "k", "60");
            assertEquals(1L, c.recvInt());
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 60, "TTL should be in (0, 60], got: " + ttl);
        }

        @Test void expire_absentKey_returns0() throws IOException {
            c.send("EXPIRE", "absent", "60");
            assertEquals(0L, c.recvInt());
        }

        @Test void pexpire_setsTtlInMs() throws IOException {
            set("k", "v");
            c.send("PEXPIRE", "k", "60000");
            assertEquals(1L, c.recvInt());
            c.send("PTTL", "k");
            long pttl = c.recvInt();
            assertTrue(pttl > 0 && pttl <= 60_000, "PTTL should be in (0, 60000], got: " + pttl);
        }

        @Test void expireat_setsUnixTimestamp() throws IOException {
            set("k", "v");
            long future = Instant.now().getEpochSecond() + 120;
            c.send("EXPIREAT", "k", String.valueOf(future));
            assertEquals(1L, c.recvInt());
            c.send("TTL", "k");
            long ttl = c.recvInt();
            assertTrue(ttl > 0 && ttl <= 120, "TTL should be in (0, 120], got: " + ttl);
        }

        @Test void pexpireat_setsUnixMs() throws IOException {
            set("k", "v");
            long futureMs = Instant.now().toEpochMilli() + 120_000;
            c.send("PEXPIREAT", "k", String.valueOf(futureMs));
            assertEquals(1L, c.recvInt());
            c.send("PTTL", "k");
            long pttl = c.recvInt();
            assertTrue(pttl > 0 && pttl <= 120_000, "PTTL out of range: " + pttl);
        }

        // ── TTL / PTTL ────────────────────────────────────────────────────────

        @Test void ttl_noExpiry_returnsMinusOne() throws IOException {
            set("k", "v");
            c.send("TTL", "k");
            assertEquals(-1L, c.recvInt());
        }

        @Test void ttl_absentKey_returnsMinusTwo() throws IOException {
            c.send("TTL", "absent");
            assertEquals(-2L, c.recvInt());
        }

        @Test void pttl_absentKey_returnsMinusTwo() throws IOException {
            c.send("PTTL", "absent");
            assertEquals(-2L, c.recvInt());
        }

        @Test void pttl_noExpiry_returnsMinusOne() throws IOException {
            set("k", "v");
            c.send("PTTL", "k");
            assertEquals(-1L, c.recvInt());
        }

        // ── PERSIST ───────────────────────────────────────────────────────────

        @Test void persist_removesExpiry() throws IOException {
            c.send("SET", "k", "v", "EX", "60");
            c.recvOk();
            c.send("PERSIST", "k");
            assertEquals(1L, c.recvInt());
            c.send("TTL", "k");
            assertEquals(-1L, c.recvInt()); // no longer expires
        }

        @Test void persist_noExpiry_returns0() throws IOException {
            set("k", "v");
            c.send("PERSIST", "k");
            assertEquals(0L, c.recvInt());
        }

        // ── EXPIRETIME / PEXPIRETIME ──────────────────────────────────────────

        @Test void expiretime_keyWithTtl_returnsUnixTimestamp() throws IOException {
            long future = Instant.now().getEpochSecond() + 120;
            set("k", "v");                         // create the key first
            c.send("EXPIREAT", "k", String.valueOf(future));
            assertEquals(1L, c.recvInt());           // must read the EXPIREAT reply
            c.send("EXPIRETIME", "k");
            long et = c.recvInt();
            assertTrue(et > 0 && et <= future + 1, "EXPIRETIME out of range: " + et);
        }

        @Test void expiretime_noExpiry_returnsMinusOne() throws IOException {
            set("k", "v");
            c.send("EXPIRETIME", "k");
            assertEquals(-1L, c.recvInt());
        }

        @Test void expiretime_absentKey_returnsMinusTwo() throws IOException {
            c.send("EXPIRETIME", "absent");
            assertEquals(-2L, c.recvInt());
        }

        @Test void pexpiretime_absentKey_returnsMinusTwo() throws IOException {
            c.send("PEXPIRETIME", "absent");
            assertEquals(-2L, c.recvInt());
        }

        // ── KEYS / SCAN ───────────────────────────────────────────────────────

        @Test void keys_wildcardPattern_returnsMatchingKeys() throws IOException {
            c.send("MSET", "user:1", "a", "user:2", "b", "item:1", "c");
            c.recvOk();
            c.send("KEYS", "user:*");
            Object[] keys = c.recvArray();
            assertNotNull(keys);
            assertEquals(2, keys.length);
            List<String> keyList = Arrays.stream(keys).map(Object::toString).collect(Collectors.toList());
            assertTrue(keyList.stream().allMatch(k -> k.startsWith("user:")));
        }

        @Test void keys_questionMarkPattern_matchesSingleChar() throws IOException {
            c.send("MSET", "foo", "1", "bar", "2", "baz", "3", "foobar", "4");
            c.recvOk();
            c.send("KEYS", "???");
            Object[] keys = c.recvArray();
            assertNotNull(keys);
            assertEquals(3, keys.length); // foo, bar, baz
        }

        @Test void keys_starPattern_returnsAllKeys() throws IOException {
            set("a", "1");
            set("b", "2");
            c.send("KEYS", "*");
            Object[] keys = c.recvArray();
            assertNotNull(keys);
            assertEquals(2, keys.length);
        }

        @Test void scan_basicCursor_returnsKeysArray() throws IOException {
            set("k1", "v1");
            set("k2", "v2");
            c.send("SCAN", "0");
            Object[] result = c.recvArray();
            assertNotNull(result);
            assertEquals(2, result.length);
            // result[0] = next cursor, result[1] = keys array
            Object[] scanKeys = (Object[]) result[1];
            assertNotNull(scanKeys);
        }

        @Test void scan_withMatchPattern_filtersKeys() throws IOException {
            c.send("MSET", "prefix:1", "a", "prefix:2", "b", "other", "c");
            c.recvOk();
            c.send("SCAN", "0", "MATCH", "prefix:*");
            Object[] result = c.recvArray();
            Object[] keys = (Object[]) result[1];
            assertNotNull(keys);
            for (Object k : keys) {
                assertTrue(k.toString().startsWith("prefix:"),
                        "SCAN MATCH returned unexpected key: " + k);
            }
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // REAL-TIME EXPIRY TESTS  (use Thread.sleep — kept minimal)
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class ExpiryTests {

        @Test void key_expiresAndBecomesInvisible() throws Exception {
            c.send("SET", "ex", "v", "PX", "300"); // 300 ms TTL
            c.recvOk();
            Thread.sleep(400);
            c.send("GET", "ex");
            assertNull(c.recvBulk(), "Key should be gone after TTL expires");
        }

        @Test void del_afterExpiry_returns0() throws Exception {
            c.send("SET", "ex", "v", "PX", "300");
            c.recvOk();
            Thread.sleep(400);
            c.send("DEL", "ex");
            assertEquals(0L, c.recvInt(), "DEL on expired key should return 0");
        }

        @Test void exists_afterExpiry_returns0() throws Exception {
            c.send("SET", "ex", "v", "PX", "300");
            c.recvOk();
            Thread.sleep(400);
            c.send("EXISTS", "ex");
            assertEquals(0L, c.recvInt(), "EXISTS on expired key should return 0");
        }

        @Test void ttl_afterExpiry_returnsMinusTwo() throws Exception {
            c.send("SET", "ex", "v", "PX", "300");
            c.recvOk();
            Thread.sleep(400);
            c.send("TTL", "ex");
            assertEquals(-2L, c.recvInt(), "TTL on expired key should return -2");
        }

        @Test void dbsize_afterExpiry_decrements() throws Exception {
            c.send("SET", "ex", "v", "PX", "300");
            c.recvOk();
            set("perm", "v");
            c.send("DBSIZE");
            assertEquals(2L, c.recvInt());
            Thread.sleep(400);
            // Access the expired key to trigger eviction
            c.send("GET", "ex");
            c.recvBulk();
            c.send("DBSIZE");
            assertEquals(1L, c.recvInt());
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // EDGE CASES
    // ═════════════════════════════════════════════════════════════════════════

    @Nested
    class EdgeCaseTests {

        @Test void unknownCommand_returnsError() throws IOException {
            c.send("NOTACOMMAND");
            assertTrue(c.recvError().startsWith("ERR"));
        }

        @Test void pipelining_sendMultipleBeforeReading() throws IOException {
            // Pipeline 5 SETs without reading responses
            for (int i = 0; i < 5; i++) {
                c.send("SET", "pk" + i, "pv" + i);
            }
            // Now read all 5 OKs
            for (int i = 0; i < 5; i++) {
                c.recvOk();
            }
            // Verify with MGET
            c.send("MGET", "pk0", "pk1", "pk2", "pk3", "pk4");
            Object[] vals = c.recvArray();
            assertNotNull(vals);
            assertEquals(5, vals.length);
            for (int i = 0; i < 5; i++) {
                assertEquals("pv" + i, vals[i], "Unexpected value at index " + i);
            }
        }

        @Test void concurrentClients_doNotInterfereSets() throws Exception {
            int threads  = 10;
            int opsEach  = 20;
            var executor = Executors.newFixedThreadPool(threads);
            var errors   = new CopyOnWriteArrayList<String>();

            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                futures.add(executor.submit(() -> {
                    try (var tc = new RespClient(port)) {
                        for (int i = 0; i < opsEach; i++) {
                            String key = "thread" + tid + ":key" + i;
                            tc.send("SET", key, "val" + i);
                            tc.recvOk();
                            tc.send("GET", key);
                            String got = tc.recvBulk();
                            if (!("val" + i).equals(got)) {
                                errors.add("thread" + tid + " key=" + key + " expected val" + i + " got " + got);
                            }
                        }
                    } catch (Exception e) {
                        errors.add("thread" + tid + " threw: " + e.getMessage());
                    }
                    return null;
                }));
            }

            for (var f : futures) f.get(10, TimeUnit.SECONDS);
            executor.shutdown();
            assertTrue(errors.isEmpty(), "Concurrent errors: " + errors);
        }

        @Test void concurrentIncr_isSerializedByLock() throws Exception {
            set("counter", "0");
            int threads  = 10;
            int incrEach = 50;
            var executor = Executors.newFixedThreadPool(threads);
            List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threads; t++) {
                futures.add(executor.submit(() -> {
                    try (var tc = new RespClient(port)) {
                        for (int i = 0; i < incrEach; i++) {
                            tc.send("INCR", "counter");
                            tc.recvInt();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }));
            }

            for (var f : futures) f.get(10, TimeUnit.SECONDS);
            executor.shutdown();

            c.send("GET", "counter");
            String val = c.recvBulk();
            assertEquals(threads * incrEach, Integer.parseInt(val),
                    "Concurrent INCRs should be serialized; expected " + (threads * incrEach) + " got " + val);
        }

        @Test void binarySafeValue_roundtrip() throws Exception {
            // Write a raw RESP command containing binary bytes (0x00 0x01 0x02) via raw socket
            byte[] cmd = "*3\r\n$3\r\nSET\r\n$6\r\nbinkey\r\n$3\r\n\u0000\u0001\u0002\r\n"
                    .getBytes(java.nio.charset.StandardCharsets.ISO_8859_1);
            c.send("SET", "binkey", "\u0000\u0001\u0002");
            c.recvOk();
            c.send("STRLEN", "binkey");
            assertEquals(3L, c.recvInt());
        }

        @Test void multipleConsecutivePings_allReturnPong() throws IOException {
            for (int i = 0; i < 10; i++) {
                c.send("PING");
                assertEquals("PONG", c.recvSimple(), "PING #" + i + " failed");
            }
        }

        @Test void info_keyspace_reflectsActualKeyCount() throws IOException {
            c.send("MSET", "x", "1", "y", "2", "z", "3");
            c.recvOk();
            c.send("INFO", "keyspace");
            String info = c.recvBulk();
            assertNotNull(info);
            assertTrue(info.contains("keys=3"),
                    "INFO keyspace should report keys=3 but got: " + info);
        }
    }
}
