import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.Executors;

public class FileOperationService {

    // Virtual-thread scheduler: blocking on a lock parks a virtual thread, not a platform thread
    private static final Scheduler IO = Schedulers.fromExecutorService(
            Executors.newVirtualThreadPerTaskExecutor(), "file-io");

    private final Path root;                       // allowed base directory
    private final PathLockManager locks = new PathLockManager(10_000);

    public FileOperationService(Path root) throws IOException {
        this.root = root.toRealPath();
    }

    public Mono<Void> renameFile(Path current, String newName) {
        return Mono.<Void>fromCallable(() -> {
                    validateName(newName);
                    Path src = canonical(current);
                    Path dst = src.resolveSibling(newName);
                    ensureInsideRoot(dst);

                    return locks.withLocks(List.of(src, dst), () -> {
                        if (!Files.isRegularFile(src, LinkOption.NOFOLLOW_LINKS)) {
                            throw new NoSuchFileException(src.toString());
                        }
                        if (Files.exists(dst, LinkOption.NOFOLLOW_LINKS)) {
                            throw new FileAlreadyExistsException(dst.toString());
                        }
                        Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
                        return null;
                    });
                })
                .subscribeOn(IO)
                .onErrorMap(FileOperationService::toHttpError);
    }

    public Mono<Void> deleteFile(Path target) {
        return Mono.<Void>fromCallable(() -> {
                    Path p = canonical(target);

                    return locks.withLocks(List.of(p), () -> {
                        if (!Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS)) {
                            throw new NoSuchFileException(p.toString());
                        }
                        Files.delete(p);
                        return null;
                    });
                })
                .subscribeOn(IO)
                .onErrorMap(FileOperationService::toHttpError);
    }

    /** Same key for "a/../x", "x" and paths through symlinked parent directories. */
    private Path canonical(Path p) throws IOException {
        Path abs = p.toAbsolutePath().normalize();
        Path result = abs.getParent().toRealPath().resolve(abs.getFileName());
        ensureInsideRoot(result);
        return result;
    }

    private void ensureInsideRoot(Path p) {
        if (!p.startsWith(root)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Path outside allowed directory");
        }
    }

    private static void validateName(String name) {
        if (name == null || name.isBlank()
                || name.equals(".") || name.equals("..")
                || !name.equals(Path.of(name).getFileName().toString())) {   // rejects separators
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid file name");
        }
    }

    private static Throwable toHttpError(Throwable t) {
        return switch (t) {
            case ResponseStatusException e -> e;
            case NoSuchFileException e -> new ResponseStatusException(HttpStatus.NOT_FOUND, "File not found");
            case FileAlreadyExistsException e -> new ResponseStatusException(HttpStatus.CONFLICT, "Target already exists");
            case PathLockManager.LockTimeoutException e -> new ResponseStatusException(HttpStatus.CONFLICT, "File is busy");
            case AtomicMoveNotSupportedException e -> new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Atomic move not supported");
            default -> t;
        };
    }
}
