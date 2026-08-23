package com.example.httpfileservice;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public final class FileServiceApp {
    private static final int DEFAULT_PORT = 8080;
    private static final long DEFAULT_MAX_UPLOAD_BYTES = 100L * 1024L * 1024L;

    private FileServiceApp() {
    }

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(System.getProperty("server.port", String.valueOf(DEFAULT_PORT)));
        long maxUploadBytes = Long.parseLong(System.getProperty("max.upload.bytes", String.valueOf(DEFAULT_MAX_UPLOAD_BYTES)));
        Path root = configuredRoot(args).toAbsolutePath().normalize();
        Files.createDirectories(root);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        FileController controller = new FileController(root, maxUploadBytes);
        server.createContext("/", controller);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();

        System.out.printf("HTTP File Service running at http://localhost:%d/%n", port);
        System.out.printf("Serving folder: %s%n", root);
    }

    private static Path configuredRoot(String[] args) {
        if (args.length > 0 && !args[0].isBlank()) {
            return Path.of(args[0]);
        }
        String propertyRoot = System.getProperty("file.root");
        if (propertyRoot != null && !propertyRoot.isBlank()) {
            return Path.of(propertyRoot);
        }
        String envRoot = System.getenv("FILE_ROOT");
        if (envRoot != null && !envRoot.isBlank()) {
            return Path.of(envRoot);
        }
        return Path.of("shared-files");
    }

    private static final class FileController implements HttpHandler {
        private static final DateTimeFormatter DATE_FORMAT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

        private final Path root;
        private final long maxUploadBytes;

        private FileController(Path root, long maxUploadBytes) {
            this.root = root;
            this.maxUploadBytes = maxUploadBytes;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                String method = exchange.getRequestMethod();
                String route = exchange.getRequestURI().getPath();

                if ("GET".equals(method) && ("/".equals(route) || "/browse".equals(route))) {
                    showDirectory(exchange);
                } else if ("GET".equals(method) && "/download".equals(route)) {
                    download(exchange);
                } else if ("POST".equals(method) && "/upload".equals(route)) {
                    upload(exchange);
                } else if ("POST".equals(method) && "/upload-stream".equals(route)) {
                    upload(exchange);
                } else if ("POST".equals(method) && "/delete".equals(route)) {
                    delete(exchange);
                } else if ("POST".equals(method) && "/mkdir".equals(route)) {
                    createDirectory(exchange);
                } else {
                    sendHtml(exchange, 404, page("Not found", "<p>The requested page was not found.</p>"));
                }
            } catch (BadRequestException exception) {
                sendHtml(exchange, 400, page("Bad request", "<p>" + escapeHtml(exception.getMessage()) + "</p>"));
            } catch (IOException exception) {
                sendHtml(exchange, 500, page("Server error", "<p>" + escapeHtml(exception.getMessage()) + "</p>"));
            }
        }

        private void showDirectory(HttpExchange exchange) throws IOException {
            Map<String, String> query = parseQuery(exchange.getRequestURI());
            Path current = resolveInsideRoot(query.getOrDefault("path", ""));
            if (!Files.isDirectory(current)) {
                throw new BadRequestException("Path is not a folder.");
            }

            String relativePath = toRelative(current);
            StringBuilder html = new StringBuilder();
            html.append("<div class=\"toolbar\">")
                    .append("<form method=\"post\" action=\"/upload-stream?path=").append(urlEncode(relativePath)).append("\" enctype=\"multipart/form-data\">")
                    .append("<input type=\"file\" name=\"file\" required>")
                    .append("<button type=\"submit\">Upload</button>")
                    .append("</form>")
                    .append("<form method=\"post\" action=\"/mkdir\">")
                    .append("<input type=\"hidden\" name=\"path\" value=\"").append(escapeHtml(relativePath)).append("\">")
                    .append("<input name=\"name\" placeholder=\"New folder\" required>")
                    .append("<button type=\"submit\">Create Folder</button>")
                    .append("</form>")
                    .append("</div>");

            html.append("<nav class=\"breadcrumbs\">").append(breadcrumbs(current)).append("</nav>");
            html.append("<table><thead><tr><th>Name</th><th>Type</th><th>Size</th><th>Modified</th><th></th></tr></thead><tbody>");

            Path parent = current.getParent();
            if (parent != null && parent.startsWith(root) && !current.equals(root)) {
                html.append("<tr><td><a href=\"/browse?path=").append(urlEncode(toRelative(parent))).append("\">..</a></td>")
                        .append("<td>Folder</td><td></td><td></td><td></td></tr>");
            }

            List<Path> children;
            try (Stream<Path> stream = Files.list(current)) {
                children = stream.sorted(directoryFirstThenName()).toList();
            }

            if (children.isEmpty()) {
                html.append("<tr><td colspan=\"5\" class=\"empty\">This folder is empty.</td></tr>");
            }

            for (Path child : children) {
                appendFileRow(html, child);
            }

            html.append("</tbody></table>");
            sendHtml(exchange, 200, page("HTTP File Service", html.toString()));
        }

        private void appendFileRow(StringBuilder html, Path child) throws IOException {
            boolean directory = Files.isDirectory(child);
            String relativePath = toRelative(child);
            String name = child.getFileName().toString();

            html.append("<tr><td>");
            if (directory) {
                html.append("<a href=\"/browse?path=").append(urlEncode(relativePath)).append("\">")
                        .append(escapeHtml(name)).append("</a>");
            } else {
                html.append("<a href=\"/download?path=").append(urlEncode(relativePath)).append("\">")
                        .append(escapeHtml(name)).append("</a>");
            }
            html.append("</td><td>").append(directory ? "Folder" : "File").append("</td>")
                    .append("<td>").append(directory ? "" : humanSize(Files.size(child))).append("</td>")
                    .append("<td>").append(DATE_FORMAT.format(Files.getLastModifiedTime(child).toInstant())).append("</td>")
                    .append("<td class=\"actions\"><form method=\"post\" action=\"/delete\" onsubmit=\"return confirm('Delete this item?');\">")
                    .append("<input type=\"hidden\" name=\"path\" value=\"").append(escapeHtml(relativePath)).append("\">")
                    .append("<button class=\"danger\" type=\"submit\">Delete</button>")
                    .append("</form></td></tr>");
        }

        private void download(HttpExchange exchange) throws IOException {
            Path file = resolveInsideRoot(parseQuery(exchange.getRequestURI()).getOrDefault("path", ""));
            if (!Files.isRegularFile(file)) {
                throw new BadRequestException("Path is not a downloadable file.");
            }

            Headers headers = exchange.getResponseHeaders();
            headers.set("Content-Type", "application/octet-stream");
            headers.set("Content-Disposition", "attachment; filename=\"" + safeHeaderFilename(file.getFileName().toString()) + "\"");
            exchange.sendResponseHeaders(200, Files.size(file));
            try (OutputStream response = exchange.getResponseBody()) {
                Files.copy(file, response);
            }
        }

        private void upload(HttpExchange exchange) throws IOException {
            Map<String, String> query = parseQuery(exchange.getRequestURI());
            Path directory = resolveInsideRoot(query.getOrDefault("path", ""));
            if (!Files.isDirectory(directory)) {
                throw new BadRequestException("Upload path is not a folder.");
            }

            String contentType = Optional.ofNullable(exchange.getRequestHeaders().getFirst("Content-Type")).orElse("");
            String boundary = multipartBoundary(contentType)
                    .orElseThrow(() -> new BadRequestException("Upload must use multipart/form-data."));
            streamMultipartUpload(exchange.getRequestBody(), boundary, directory);
            redirect(exchange, "/browse?path=" + urlEncode(toRelative(directory)));
        }

        private void delete(HttpExchange exchange) throws IOException {
            Map<String, String> form = parseForm(exchange);
            Path target = resolveInsideRoot(form.getOrDefault("path", ""));
            if (target.equals(root)) {
                throw new BadRequestException("The root folder cannot be deleted.");
            }
            Path parent = target.getParent() == null ? root : target.getParent();

            if (!Files.exists(target)) {
                throw new BadRequestException("Path does not exist.");
            }
            deleteRecursively(target);
            redirect(exchange, "/browse?path=" + urlEncode(toRelative(parent)));
        }

        private void createDirectory(HttpExchange exchange) throws IOException {
            Map<String, String> form = parseForm(exchange);
            Path directory = resolveInsideRoot(form.getOrDefault("path", ""));
            if (!Files.isDirectory(directory)) {
                throw new BadRequestException("Path is not a folder.");
            }
            String name = form.getOrDefault("name", "").trim();
            if (name.isEmpty()) {
                throw new BadRequestException("Folder name is required.");
            }
            Path target = directory.resolve(safeFileName(name)).normalize();
            ensureInsideRoot(target);
            Files.createDirectory(target);
            redirect(exchange, "/browse?path=" + urlEncode(toRelative(directory)));
        }

        private void deleteRecursively(Path target) throws IOException {
            if (Files.isDirectory(target)) {
                try (Stream<Path> stream = Files.walk(target)) {
                    List<Path> paths = stream.sorted(Comparator.reverseOrder()).toList();
                    for (Path path : paths) {
                        Files.delete(path);
                    }
                }
            } else {
                Files.delete(target);
            }
        }

        private Path resolveInsideRoot(String relativePath) {
            try {
                Path requested = relativePath == null || relativePath.isBlank() ? root : root.resolve(relativePath).normalize();
                ensureInsideRoot(requested);
                return requested;
            } catch (InvalidPathException exception) {
                throw new BadRequestException("Invalid path.");
            }
        }

        private void ensureInsideRoot(Path path) {
            if (!path.normalize().startsWith(root)) {
                throw new BadRequestException("Path must stay inside the configured folder.");
            }
        }

        private String toRelative(Path path) {
            Path relative = root.relativize(path.toAbsolutePath().normalize());
            return relative.toString().replace('\\', '/');
        }

        private String breadcrumbs(Path current) {
            StringBuilder builder = new StringBuilder();
            builder.append("<a href=\"/browse\">Home</a>");
            Path relative = root.relativize(current);
            Path accumulated = root;
            for (Path part : relative) {
                accumulated = accumulated.resolve(part);
                builder.append("<span>/</span><a href=\"/browse?path=")
                        .append(urlEncode(toRelative(accumulated)))
                        .append("\">")
                        .append(escapeHtml(part.toString()))
                        .append("</a>");
            }
            return builder.toString();
        }

        private Map<String, String> parseForm(HttpExchange exchange) throws IOException {
            String body = new String(readLimited(exchange.getRequestBody(), 64 * 1024), StandardCharsets.UTF_8);
            Map<String, String> form = new HashMap<>();
            for (String pair : body.split("&")) {
                if (pair.isBlank()) {
                    continue;
                }
                String[] parts = pair.split("=", 2);
                String key = urlDecode(parts[0]);
                String value = parts.length > 1 ? urlDecode(parts[1]) : "";
                form.put(key, value);
            }
            return form;
        }

        private Map<String, String> parseQuery(URI uri) {
            Map<String, String> query = new HashMap<>();
            String rawQuery = uri.getRawQuery();
            if (rawQuery == null || rawQuery.isBlank()) {
                return query;
            }
            for (String pair : rawQuery.split("&")) {
                String[] parts = pair.split("=", 2);
                query.put(urlDecode(parts[0]), parts.length > 1 ? urlDecode(parts[1]) : "");
            }
            return query;
        }

        private byte[] readLimited(InputStream input, long maxBytes) throws IOException {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            long total = 0;
            int read;
            while ((read = input.read(buffer)) != -1) {
                total += read;
                if (total > maxBytes) {
                    throw new BadRequestException("Request body is too large.");
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }

        private Optional<String> multipartBoundary(String contentType) {
            for (String token : contentType.split(";")) {
                String trimmed = token.trim();
                if (trimmed.startsWith("boundary=")) {
                    String boundary = trimmed.substring("boundary=".length());
                    if (boundary.startsWith("\"") && boundary.endsWith("\"")) {
                        boundary = boundary.substring(1, boundary.length() - 1);
                    }
                    return Optional.of(boundary);
                }
            }
            return Optional.empty();
        }

        private void streamMultipartUpload(InputStream input, String boundary, Path directory) throws IOException {
            String boundaryLine = "--" + boundary;
            expectOpeningBoundary(input, boundaryLine);

            boolean finalBoundary = false;
            while (!finalBoundary) {
                String headers = readHeaders(input);
                String disposition = headerValue(headers, "Content-Disposition").orElse("");
                boolean uploadPart = disposition.contains("name=\"file\"");
                Optional<String> fileName = dispositionParameter(disposition, "filename");

                if (uploadPart && fileName.isPresent()) {
                    streamFilePart(input, boundary, directory, fileName.get());
                    return;
                }

                finalBoundary = copyPartBody(input, boundary, OutputStream.nullOutputStream()).finalBoundary();
            }

            throw new BadRequestException("No uploaded file was found.");
        }

        private void expectOpeningBoundary(InputStream input, String boundaryLine) throws IOException {
            String line;
            while ((line = readLine(input, 8192)) != null) {
                if (line.equals(boundaryLine)) {
                    return;
                }
                if (line.equals(boundaryLine + "--")) {
                    throw new BadRequestException("No uploaded file was found.");
                }
            }
            throw new BadRequestException("Multipart boundary was not found.");
        }

        private String readHeaders(InputStream input) throws IOException {
            StringBuilder headers = new StringBuilder();
            String line;
            while ((line = readLine(input, 8192)) != null) {
                if (line.isEmpty()) {
                    return headers.toString();
                }
                if (headers.length() + line.length() > 64 * 1024) {
                    throw new BadRequestException("Multipart headers are too large.");
                }
                headers.append(line).append("\r\n");
            }
            throw new BadRequestException("Unexpected end of multipart headers.");
        }

        private String readLine(InputStream input, int maxBytes) throws IOException {
            ByteArrayOutputStream line = new ByteArrayOutputStream();
            int next;
            while ((next = input.read()) != -1) {
                if (next == '\n') {
                    return line.toString(StandardCharsets.ISO_8859_1);
                }
                if (next != '\r') {
                    line.write(next);
                }
                if (line.size() > maxBytes) {
                    throw new BadRequestException("Multipart line is too large.");
                }
            }
            if (line.size() == 0) {
                return null;
            }
            return line.toString(StandardCharsets.ISO_8859_1);
        }

        private void streamFilePart(InputStream input, String boundary, Path directory, String rawFileName) throws IOException {
            if (rawFileName.isBlank()) {
                throw new BadRequestException("Uploaded file must have a filename.");
            }

            Path target = directory.resolve(safeFileName(rawFileName)).normalize();
            ensureInsideRoot(target);
            Path tempFile = Files.createTempFile(directory, ".upload-", ".part");

            try {
                try (OutputStream output = Files.newOutputStream(tempFile, StandardOpenOption.WRITE)) {
                    copyPartBody(input, boundary, output);
                }
                moveUploadIntoPlace(tempFile, target);
            } catch (IOException | RuntimeException exception) {
                Files.deleteIfExists(tempFile);
                throw exception;
            }
        }

        private void moveUploadIntoPlace(Path tempFile, Path target) throws IOException {
            try {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicMoveFailure) {
                Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
            }
        }

        private PartCopyResult copyPartBody(InputStream input, String boundary, OutputStream output) throws IOException {
            byte[] delimiter = ("\r\n--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
            byte[] pending = new byte[delimiter.length];
            int pendingLength = 0;
            long written = 0;

            while (true) {
                int next = input.read();
                if (next == -1) {
                    throw new BadRequestException("Unexpected end of multipart body.");
                }

                pending[pendingLength++] = (byte) next;
                while (pendingLength > 0 && !isPrefix(delimiter, pending, pendingLength)) {
                    if (written + 1 > maxUploadBytes) {
                        throw new BadRequestException("Uploaded file is too large.");
                    }
                    output.write(pending[0]);
                    written++;
                    System.arraycopy(pending, 1, pending, 0, pendingLength - 1);
                    pendingLength--;
                }

                if (pendingLength == delimiter.length) {
                    return new PartCopyResult(written, consumeBoundaryEnding(input));
                }
            }
        }

        private boolean consumeBoundaryEnding(InputStream input) throws IOException {
            int first = input.read();
            if (first == '-') {
                int second = input.read();
                if (second != '-') {
                    throw new BadRequestException("Malformed multipart boundary.");
                }
                readLine(input, 1024);
                return true;
            }
            if (first == '\r') {
                int second = input.read();
                if (second != '\n') {
                    throw new BadRequestException("Malformed multipart boundary.");
                }
                return false;
            }
            if (first == '\n') {
                return false;
            }
            throw new BadRequestException("Malformed multipart boundary.");
        }

        private boolean isPrefix(byte[] expected, byte[] actual, int actualLength) {
            if (actualLength > expected.length) {
                return false;
            }
            for (int index = 0; index < actualLength; index++) {
                if (actual[index] != expected[index]) {
                    return false;
                }
            }
            return true;
        }

        private Optional<String> headerValue(String headers, String headerName) {
            for (String line : headers.split("\r\n")) {
                int colon = line.indexOf(':');
                if (colon > 0 && line.substring(0, colon).equalsIgnoreCase(headerName)) {
                    return Optional.of(line.substring(colon + 1).trim());
                }
            }
            return Optional.empty();
        }

        private Optional<String> dispositionParameter(String disposition, String name) {
            String prefix = name + "=\"";
            int start = disposition.indexOf(prefix);
            if (start < 0) {
                return Optional.empty();
            }
            start += prefix.length();
            int end = disposition.indexOf('"', start);
            if (end < 0) {
                return Optional.empty();
            }
            return Optional.of(disposition.substring(start, end));
        }

        private Comparator<Path> directoryFirstThenName() {
            return Comparator
                    .comparing((Path path) -> !Files.isDirectory(path))
                    .thenComparing(path -> path.getFileName().toString().toLowerCase(Locale.ROOT));
        }

        private String page(String title, String content) {
            return "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
                    + "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
                    + "<title>" + escapeHtml(title) + "</title>"
                    + "<style>"
                    + ":root{color-scheme:light dark;font-family:system-ui,-apple-system,BlinkMacSystemFont,\"Segoe UI\",sans-serif;}"
                    + "body{margin:0;background:#f5f7fb;color:#1d2433;}main{max-width:1100px;margin:0 auto;padding:32px 20px;}"
                    + "h1{font-size:28px;margin:0 0 20px;}a{color:#165dff;text-decoration:none;}a:hover{text-decoration:underline;}"
                    + ".toolbar{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap;margin-bottom:16px;}"
                    + "form{display:flex;gap:8px;align-items:center;margin:0;}input{font:inherit;padding:9px 10px;border:1px solid #cbd3df;border-radius:6px;background:white;color:#1d2433;}"
                    + "button{font:inherit;border:0;border-radius:6px;background:#1957d2;color:white;padding:10px 14px;cursor:pointer;}button:hover{background:#1347ad;}"
                    + "button.danger{background:#bd2d2d;}button.danger:hover{background:#9e2020;}"
                    + ".breadcrumbs{margin:12px 0 18px;color:#5a6475;}.breadcrumbs span{margin:0 8px;color:#7b8494;}"
                    + "table{width:100%;border-collapse:collapse;background:white;border:1px solid #dce2ea;}th,td{padding:12px;border-bottom:1px solid #e6ebf2;text-align:left;}"
                    + "th{font-size:13px;text-transform:uppercase;color:#5a6475;background:#f0f3f8;}td.actions{width:1%;white-space:nowrap;}.empty{text-align:center;color:#697386;padding:32px;}"
                    + "@media (prefers-color-scheme:dark){body{background:#111827;color:#f3f4f6;}input,table{background:#172033;color:#f3f4f6;border-color:#314158;}th{background:#202b3d;color:#c7d2e2;}td,th{border-color:#2f3b50;}a{color:#8ab4ff;}}"
                    + "@media (max-width:720px){main{padding:20px 12px;}table{font-size:14px;}th:nth-child(3),td:nth-child(3),th:nth-child(4),td:nth-child(4){display:none;}form{width:100%;}input[type=file],input[name=name]{min-width:0;flex:1;}}"
                    + "</style></head><body><main><h1>" + escapeHtml(title) + "</h1>"
                    + content
                    + "</main></body></html>";
        }

        private void sendHtml(HttpExchange exchange, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream response = exchange.getResponseBody()) {
                response.write(bytes);
            }
        }

        private void redirect(HttpExchange exchange, String location) throws IOException {
            exchange.getResponseHeaders().set("Location", location);
            exchange.sendResponseHeaders(303, -1);
            exchange.close();
        }

        private String safeFileName(String fileName) {
            String normalized = fileName.replace('\\', '/');
            String name = Path.of(normalized).getFileName().toString();
            if (name.equals(".") || name.equals("..") || name.contains("/") || name.contains("\\")) {
                throw new BadRequestException("Invalid filename.");
            }
            return name;
        }

        private String safeHeaderFilename(String fileName) {
            return fileName.replace("\\", "\\\\").replace("\"", "\\\"");
        }

        private String humanSize(long bytes) {
            if (bytes < 1024) {
                return bytes + " B";
            }
            double value = bytes;
            String[] units = {"KB", "MB", "GB", "TB"};
            for (String unit : units) {
                value /= 1024.0;
                if (value < 1024.0) {
                    return String.format(Locale.ROOT, "%.1f %s", value, unit);
                }
            }
            return String.format(Locale.ROOT, "%.1f PB", value / 1024.0);
        }

        private String urlEncode(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
        }

        private String urlDecode(String value) {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        }

        private String escapeHtml(String value) {
            return value == null ? "" : value
                    .replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace("\"", "&quot;")
                    .replace("'", "&#39;");
        }
    }

    private record PartCopyResult(long bytesWritten, boolean finalBoundary) {
    }

    private static final class BadRequestException extends RuntimeException {
        private BadRequestException(String message) {
            super(message);
        }
    }
}
