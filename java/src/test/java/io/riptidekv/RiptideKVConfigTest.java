package io.riptidekv;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RiptideKVConfig} and its builder.
 * No server process is started — purely tests the Java config object.
 */
class RiptideKVConfigTest {

    // ── Default values ────────────────────────────────────────────────────────

    @Test
    void defaults_bindIsLocalhost6379() {
        var cfg = RiptideKVConfig.builder().build();
        assertEquals("127.0.0.1:6379", cfg.getBind());
    }

    @Test
    void defaults_flushKbIs1024() {
        var cfg = RiptideKVConfig.builder().build();
        assertEquals(1024, cfg.getFlushKb());
    }

    @Test
    void defaults_walSyncIsTrue() {
        var cfg = RiptideKVConfig.builder().build();
        assertTrue(cfg.isWalSync());
    }

    @Test
    void defaults_dataDirIsUnderTmpdir() {
        var cfg = RiptideKVConfig.builder().build();
        assertTrue(cfg.getDataDir().toString().contains("riptidekv"),
                "default dataDir should contain 'riptidekv', got: " + cfg.getDataDir());
    }

    // ── Port extraction ───────────────────────────────────────────────────────

    @Test
    void getPort_extractsFromDefaultBind() {
        assertEquals(6379, RiptideKVConfig.builder().build().getPort());
    }

    @Test
    void getPort_extractsCustomPort() {
        var cfg = RiptideKVConfig.builder().bind("127.0.0.1:6380").build();
        assertEquals(6380, cfg.getPort());
    }

    @Test
    void getPort_worksWithAllInterfaces() {
        var cfg = RiptideKVConfig.builder().bind("0.0.0.0:9999").build();
        assertEquals(9999, cfg.getPort());
    }

    // ── Custom values ─────────────────────────────────────────────────────────

    @Test
    void customBind_isStored() {
        var cfg = RiptideKVConfig.builder().bind("0.0.0.0:7777").build();
        assertEquals("0.0.0.0:7777", cfg.getBind());
    }

    @Test
    void customDataDir_isStored() {
        var dir = Paths.get("/tmp/mydb");
        var cfg = RiptideKVConfig.builder().dataDir(dir).build();
        assertEquals(dir, cfg.getDataDir());
    }

    @Test
    void customFlushKb_isStored() {
        var cfg = RiptideKVConfig.builder().flushKb(4096).build();
        assertEquals(4096, cfg.getFlushKb());
    }

    @Test
    void walSyncFalse_isStored() {
        var cfg = RiptideKVConfig.builder().walSync(false).build();
        assertFalse(cfg.isWalSync());
    }

    // ── Validation ────────────────────────────────────────────────────────────

    @Test
    void flushKbZero_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().flushKb(0).build());
    }

    @Test
    void flushKbNegative_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().flushKb(-1).build());
    }

    @Test
    void nullDataDir_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().dataDir(null).build());
    }

    @Test
    void blankBind_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().bind("").build());
    }

    @Test
    void nullBind_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().bind(null).build());
    }

    // ── Builder fluency ───────────────────────────────────────────────────────

    @Test
    void invalidBindNoColon_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().bind("localhost").build());
    }

    @Test
    void invalidBindNonNumericPort_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().bind("127.0.0.1:abc").build());
    }

    @Test
    void invalidBindPortOutOfRange_throwsIllegalArgument() {
        assertThrows(IllegalArgumentException.class,
                () -> RiptideKVConfig.builder().bind("127.0.0.1:99999").build());
    }

    @Test
    void builder_isFullyFluent() {
        var cfg = RiptideKVConfig.builder()
                .bind("127.0.0.1:16379")
                .dataDir(Paths.get("/tmp/test"))
                .flushKb(512)
                .walSync(false)
                .build();

        assertEquals("127.0.0.1:16379", cfg.getBind());
        assertEquals(Paths.get("/tmp/test"), cfg.getDataDir());
        assertEquals(512, cfg.getFlushKb());
        assertFalse(cfg.isWalSync());
        assertEquals(16379, cfg.getPort());
    }
}
