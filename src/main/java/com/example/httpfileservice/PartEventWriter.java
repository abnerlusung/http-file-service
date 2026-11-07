import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PartEventWriter {

    private PartEventWriter() {}

    public static Mono<Void> write(String destination, Flux<PartEvent> partEvents) {
        Path target = Path.of(destination).toAbsolutePath().normalize();
        Path temp = target.resolveSibling(target.getFileName() + "." + UUID.randomUUID() + ".part");

        return Mono.defer(() -> {
            // Identity-based: NettyDataBuffer.equals/hashCode are content-based
            Set<DataBuffer> inFlight =
                    Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
            AtomicBoolean inFilePart = new AtomicBoolean();

            Flux<DataBuffer> content = partEvents
                    .<PartEvent>handle((event, sink) -> {
                        if (event instanceof FilePartEvent) {
                            inFilePart.set(true);               // first file part starts
                        }
                        if (inFilePart.get()) {
                            sink.next(event);                    // all chunks of that part
                        } else {
                            DataBufferUtils.release(event.content()); // skipped form fields
                        }
                    })
                    .takeUntil(PartEvent::isLast)                // stop after first file part
                    .map(PartEvent::content)
                    .doOnNext(inFlight::add)
                    // Hooks go LAST so they cover every operator above
                    .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                    .doOnDiscard(PartEvent.class, e -> DataBufferUtils.release(e.content()));

            return Mono.using(
                            () -> open(temp),
                            channel -> DataBufferUtils.write(content, channel, 0)
                                    .doOnNext(buffer -> {
                                        inFlight.remove(buffer);
                                        DataBufferUtils.release(buffer);
                                    })
                                    .then(),
                            PartEventWriter::close)
                    .publishOn(Schedulers.boundedElastic())      // leave the channel callback thread
                    .then(Mono.fromRunnable(() -> {
                        if (!inFilePart.get()) {
                            throw new IllegalArgumentException("No file part in request");
                        }
                        move(temp, target);
                    }))
                    .doFinally(signal -> {
                        // Safety net: release anything written-but-not-released, or in flight at cancel/error.
                        // DataBufferUtils.release is a no-op for already-released buffers.
                        synchronized (inFlight) {
                            inFlight.forEach(DataBufferUtils::release);
                            inFlight.clear();
                        }
                        if (signal != SignalType.ON_COMPLETE) {
                            deleteQuietly(temp);                 // no partial files
                        }
                    });
        }).subscribeOn(Schedulers.boundedElastic());             // blocking open/mkdirs
    }

    private static AsynchronousFileChannel open(Path temp) {
        try {
            Files.createDirectories(temp.getParent());
            return AsynchronousFileChannel.open(temp,
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void close(AsynchronousFileChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
        }
    }

    private static void move(Path temp, Path target) {
        try {
            Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException ignored) {
        }
    }
}
