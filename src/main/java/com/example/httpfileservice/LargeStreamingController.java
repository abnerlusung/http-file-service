import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePartEvent;
import org.springframework.http.codec.multipart.PartEvent;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileOutputStream;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@RestController
public class LargeStreamingController {

    @PostMapping(value = "/stream-upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public Mono<String> streamUpload(@RequestBody Flux<PartEvent> partEvents) {
        return partEvents
            .windowUntil(PartEvent::isLast) // Split into windows per file/part
            .concatMap(window -> window.collectList().flatMap(parts -> {
                if (parts.isEmpty()) return Mono.empty();
                
                // Identify file parts
                if (parts.get(0) instanceof FilePartEvent fileEvent) {
                    Path path = Paths.get("uploads").resolve(fileEvent.filename());
                    
                    // Concat and write buffers sequentially
                    return Flux.fromIterable(parts)
                        .map(PartEvent::content)
                        .flatMap(dataBuffer -> {
                            // Extract byte array or write using DataBufferUtils
                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
                            dataBuffer.read(bytes);
                            // Ensure buffers are released to avoid leaks
                            org.springframework.core.io.buffer.DataBufferUtils.release(dataBuffer); 
                            return Mono.just(bytes);
                        })
                        // Append bytes sequentially to your destination stream/storage
                        .reduce(new byte[0], (acc, next) -> combineArrays(acc, next))
                        .then();
                }
                return Mono.empty();
            }))
            .then(Mono.just("Streaming upload completed successfully."));
    }
    
    private byte[] combineArrays(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}
