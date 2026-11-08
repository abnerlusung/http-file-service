import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class PartEventWriter2 {

    public static final long DEFAULT_MAX_BYTES = 1L << 30;                       // 1 GiB
    public static final Duration DEFAULT_IDLE_TIMEOUT = Duration.ofSeconds(30);  // max gap between events

    private static final Scheduler IO = Schedulers.boundedElastic();

    private PartEventWriter() {}

    /** Per-subscription state, so the returned Mono is safe to subscribe more than once. */
    private static final class State {
        final AtomicBoolean created = new AtomicBoolean();   // true only if WE created the file
        final AtomicBoolean fileSeen = new AtomicBoolean();
        final AtomicBoolean lastSeen = new AtomicBoolean();
        final AtomicLong written = new AtomicLong();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        void fail(Throwable t) { failure.compareAndSet(null, t); }
    }

    public static Mono<Void> write(String destination, Flux<PartEvent> partEvents) {
        return write(destination, partEvents, DEFAULT_MAX_BYTES, DEFAULT_IDLE_TIMEOUT);
    }

    public static Mono<Void> write(String destination, Flux<PartEvent> partEvents,
                                   long maxBytes, Duration idleTimeout) {
        Objects.requireNonNull(destination, "destination");
        Objects.requireNonNull(partEvents, "partEvents");
        Path target = Path.of(destination).toAbsolutePath().normalize();

        return Mono.defer(() -> {
                    State state = new State();

                    return Mono.using(
                                    () -> open(target, state),
                                    channel -> pipeline(channel, partEvents, state, maxBytes, idleTimeout),
                                    PartEventWriter::close)       // eager: channel is closed BEFORE the terminal signal
                            // Error path: delete our partial file BEFORE the error reaches the caller
                            .onErrorResume(ex -> state.created.get()
                                    ? Mono.<Void>fromRunnable(() -> deleteQuietly(target))
                                            .subscribeOn(IO)
                                            .then(Mono.<Void>error(ex))
                                    : Mono.<Void>error(ex))
                            // Cancel path: channel is already closed here (cancel reaches `using` first)
                            .doFinally(signal -> {
                                if (signal == SignalType.CANCEL && state.created.get()) {
                                    IO.schedule(() -> deleteQuietly(target));
                                }
                            });
                })
                .subscribeOn(IO);                                  // blocking mkdirs/open off the caller thread
    }

    private static Mono<Void> pipeline(FileChannel channel, Flux<PartEvent> partEvents,
                                       State state, long maxBytes, Duration idleTimeout) {
        return partEvents
                .timeout(idleTimeout)                              // stalled client
                // Skip everything before the first file part. Skipped content is released here, exactly once.
                .<PartEvent>handle((event, sink) -> {
                    if (event instanceof FilePartEvent) {
                        state.fileSeen.set(true);
                    }
                    if (state.fileSeen.get()) {
                        if (event.isLast()) {
                            state.lastSeen.set(true);
                        }
                        sink.next(event);
                    } else {
                        DataBufferUtils.release(event.content());
                    }
                })
                .takeUntil(PartEvent::isLast)                      // first file part only
                .map(PartEvent::content)
                .publishOn(IO, 1)                                  // prefetch 1: bounded memory
                .<Void>handle((buffer, sink) -> writeChunk(channel, buffer, state, maxBytes, sink))
                // Hooks LAST so they cover every operator above (queues, takeUntil, publishOn, cancel, error)
                .doOnDiscard(PartEvent.class, e -> DataBufferUtils.release(e.content()))
                .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                .then(Mono.defer(() -> verify(state)));
    }

    /**
     * Owns the buffer: writes it and releases it in finally, on this thread, once.
     * Never throws and never calls sink.error: failures are recorded and the stream is completed,
     * so no operator can discard (double release) a buffer we already released.
     */
    private static void writeChunk(FileChannel channel, DataBuffer buffer, State state,
                                   long maxBytes, SynchronousSink<Void> sink) {
        try {
            if (state.failure.get() != null) {
                return;                                            // already failed: just release (finally)
            }
            long total = state.written.addAndGet(buffer.readableByteCount());
            if (total > maxBytes) {
                state.fail(new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "Upload too large"));
                sink.complete();                                   // cancels upstream; queued items are discarded
                return;
            }
            writeFully(channel, buffer);
        } catch (IOException e) {                                  // incl. disk full, closed/interrupted on cancel
            state.fail(new UncheckedIOException(e));
            sink.complete();
        } finally {
            DataBufferUtils.release(buffer);
        }
    }

    private static void writeFully(FileChannel channel, DataBuffer buffer) throws IOException {
        try (DataBuffer.ByteBufferIterator it = buffer.readableByteBuffers()) {
            while (it.hasNext()) {
                ByteBuffer bb = it.next();
                while (bb.hasRemaining()) {                        // write() may be partial
                    channel.write(bb);
                }
            }
        }
    }

    private static Mono<Void> verify(State state) {
        Throwable failure = state.failure.get();
        if (failure != null) {
            return Mono.error(failure);
        }
        if (!state.fileSeen.get()) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "No file part in request"));
        }
        if (!state.lastSeen.get()) {                               // stream ended before the part's last event
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "Incomplete file part"));
        }
        return Mono.empty();
    }

    private static FileChannel open(Path target, State state) {
        try {
            Path parent = target.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            // CREATE_NEW: never overwrite, never follow a symlink at the final component
            FileChannel channel = FileChannel.open(target,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            state.created.set(true);                               // from here on the file is ours to delete
            return channel;
        } catch (FileAlreadyExistsException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "Destination already exists", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void close(FileChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);                     // surfaces flush/close failure on success path
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }
}
