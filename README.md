# HTTP File Service

A small Maven-based Java web app for browsing a configured folder over HTTP. Users can view folders, download files, upload files, create folders, and delete files or folders from the browser.

## Requirements

- Java 21
- Maven 3.9+

## Run

Use the default folder, `shared-files`:

```bash
mvn package
java -jar target/http-file-service-1.0.0.jar
```

Use a specific folder:

```bash
mvn package
java -Dfile.root=/path/to/folder -jar target/http-file-service-1.0.0.jar
```

Or pass the folder as the first argument:

```bash
java -jar target/http-file-service-1.0.0.jar /path/to/folder
```

Then open:

```text
http://localhost:8080/
```

## Configuration

- `file.root`: Folder to expose over HTTP. Defaults to `shared-files`.
- `server.port`: HTTP port. Defaults to `8080`.
- `max.upload.bytes`: Maximum upload request size. Defaults to `104857600` bytes.

Example:

```bash
java -Dfile.root=/Users/me/Documents/PublicFiles -Dserver.port=9090 -Dmax.upload.bytes=52428800 -jar target/http-file-service-1.0.0.jar
```

## HTTP Endpoints

- `GET /browse?path=folder`: Browse a folder.
- `GET /download?path=file.txt`: Download a file.
- `POST /upload-stream?path=folder`: Upload a multipart form file using streaming.
- `POST /upload?path=folder`: Compatibility alias for the streaming upload handler.
- `POST /mkdir`: Create a folder with URL-encoded `path` and `name` form fields.
- `POST /delete`: Delete a file or folder with a URL-encoded `path` form field.

## Security Notes

The app normalizes all requested paths and rejects operations outside the configured root folder. This project is intended for trusted local or internal-network use. Add authentication, HTTPS, and audit logging before exposing it on the public internet.
