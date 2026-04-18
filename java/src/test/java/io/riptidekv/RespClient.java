package io.riptidekv;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Minimal RESP2 client for tests only.
 * Reads responses at the byte level so binary values are handled correctly.
 */
class RespClient implements AutoCloseable {

    private final Socket         socket;
    private final OutputStream   out;
    private final InputStream    in;

    RespClient(int port) throws IOException {
        this("127.0.0.1", port);
    }

    RespClient(String host, int port) throws IOException {
        socket = new Socket(host, port);
        socket.setSoTimeout(5_000);
        out = new BufferedOutputStream(socket.getOutputStream());
        in  = socket.getInputStream();
    }

    // ── Send ─────────────────────────────────────────────────────────────────

    /** Write a RESP2 inline array command. */
    void send(String... args) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append('*').append(args.length).append("\r\n");
        for (String arg : args) {
            byte[] b = arg.getBytes(StandardCharsets.UTF_8);
            sb.append('$').append(b.length).append("\r\n").append(arg).append("\r\n");
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    // ── Receive ───────────────────────────────────────────────────────────────

    /**
     * Read one RESP2 value.  Returns:
     * <ul>
     *   <li>{@code String}   — simple string (+) or bulk string ($)
     *   <li>{@code Long}     — integer (:)
     *   <li>{@code Object[]} — array (*)
     *   <li>{@code null}     — null bulk ($-1) or null array (*-1)
     *   <li>{@link RespError}— error (-)
     * </ul>
     */
    Object recv() throws IOException {
        String line    = readLine();
        char   type    = line.charAt(0);
        String payload = line.substring(1);

        return switch (type) {
            case '+' -> payload;
            case '-' -> new RespError(payload);
            case ':' -> Long.parseLong(payload);
            case '$' -> {
                int len = Integer.parseInt(payload);
                if (len == -1) yield null;
                byte[] buf = in.readNBytes(len);
                in.readNBytes(2); // CRLF
                yield new String(buf, StandardCharsets.UTF_8);
            }
            case '*' -> {
                int count = Integer.parseInt(payload);
                if (count == -1) yield null;
                Object[] arr = new Object[count];
                for (int i = 0; i < count; i++) arr[i] = recv();
                yield arr;
            }
            default -> throw new IOException("Unknown RESP prefix '" + type + "' in: " + line);
        };
    }

    // ── Typed receive helpers ─────────────────────────────────────────────────

    /** Assert the next response is a simple string; return it. */
    String recvSimple() throws IOException {
        Object r = recv();
        if (r instanceof String s) return s;
        throw new AssertionError("Expected simple string, got: " + r);
    }

    /** Assert the next response is +OK. */
    void recvOk() throws IOException {
        String s = recvSimple();
        if (!"OK".equals(s)) throw new AssertionError("Expected OK, got: " + s);
    }

    /** Read a bulk string; may be null (null bulk reply). */
    String recvBulk() throws IOException {
        Object r = recv();
        if (r == null || r instanceof String) return (String) r;
        throw new AssertionError("Expected bulk string, got: " + r);
    }

    /** Read an integer reply. */
    long recvInt() throws IOException {
        Object r = recv();
        if (r instanceof Long l) return l;
        throw new AssertionError("Expected integer, got: " + r);
    }

    /** Read an array reply; may be null. */
    Object[] recvArray() throws IOException {
        Object r = recv();
        if (r == null || r instanceof Object[]) return (Object[]) r;
        throw new AssertionError("Expected array, got: " + r);
    }

    /** Read an error reply. */
    RespError recvError() throws IOException {
        Object r = recv();
        if (r instanceof RespError e) return e;
        throw new AssertionError("Expected error, got: " + r);
    }

    // ── Low-level ─────────────────────────────────────────────────────────────

    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                in.read(); // consume \n
                return baos.toString(StandardCharsets.UTF_8);
            }
            baos.write(b);
        }
        throw new EOFException("Server closed connection mid-line");
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    // ── Error wrapper ─────────────────────────────────────────────────────────

    record RespError(String message) {
        boolean startsWith(String prefix) { return message.startsWith(prefix); }
        @Override public String toString() { return "-" + message; }
    }
}
