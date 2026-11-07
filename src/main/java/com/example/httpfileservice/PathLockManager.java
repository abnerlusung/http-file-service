import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public final class PathLockManager {

    private static final class Entry {
        final ReentrantLock lock = new ReentrantLock();
        int refs;                         // only mutated inside ConcurrentHashMap.compute*
    }

    private final ConcurrentHashMap<Path, Entry> locks = new ConcurrentHashMap<>();
    private final long timeoutMillis;

    public PathLockManager(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    /** Runs the action while holding the locks for all paths. Blocking: call from a worker thread. */
    public <T> T withLocks(Collection<Path> paths, Callable<T> action) throws Exception {
        List<Path> sorted = paths.stream().distinct().sorted().toList();   // global order = no deadlock
        List<Path> counted = new ArrayList<>(sorted.size());
        List<Entry> held = new ArrayList<>(sorted.size());
        try {
            for (Path p : sorted) {
                Entry e = locks.compute(p, (k, v) -> {
                    Entry entry = (v == null) ? new Entry() : v;
                    entry.refs++;
                    return entry;
                });
                counted.add(p);
                if (!e.lock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS)) {   // interruptible
                    throw new LockTimeoutException("Timed out waiting for lock on " + p);
                }
                held.add(e);
            }
            return action.call();
        } finally {
            for (int i = held.size() - 1; i >= 0; i--) {
                held.get(i).lock.unlock();
            }
            for (Path p : counted) {
                locks.computeIfPresent(p, (k, v) -> --v.refs == 0 ? null : v);  // evict unused
            }
        }
    }

    public static final class LockTimeoutException extends RuntimeException {
        public LockTimeoutException(String message) { super(message); }
    }
}
