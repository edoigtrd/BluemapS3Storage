package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.core.storage.MapStorage;
import de.bluecolored.bluemap.core.storage.Storage;
import de.bluecolored.bluemap.core.storage.compression.Compression;
import de.bluecolored.bluemap.core.storage.file.FileMapStorage;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class S3Storage implements Storage {
    IS3Configuration config;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private FileSystem S3fileSystem;

    BlobStore blobStore;

    public FileMapStorage create(String mapId) {
        Path mapPath = getRootPath().resolve(mapId);
        return new FileMapStorage(mapPath, Compression.NONE, false);
    }


    public S3Storage(IS3Configuration config) {
            this.config = config;
    }

    private Path getRootPath() {
        return S3fileSystem.getPath(config.getPrefix());
    }

    @Override
    public void initialize() throws IOException {

        Properties props = new Properties();
        props.setProperty("jclouds.s3.virtual-host-buckets", "false");
        props.setProperty("jclouds.max-retries", "3");
        props.setProperty("jclouds.request-timeout", "120000");
        props.setProperty("jclouds.so-timeout", "120000");

        BlobStoreContext ctx = ContextBuilder.newBuilder("s3")
                .credentials(config.getAccessKey(), config.getSecretKey())
                .endpoint(config.getEndpoint())
                .overrides(props)
                .buildView(BlobStoreContext.class);

        this.blobStore = ctx.getBlobStore();
    }

    @Override
    public MapStorage map(String s) {
        MapStorage ms = new S3MapStorage(
            this
        );
        return ms;
    }

    @Override
    public Stream<String> mapIds() throws IOException {
        return Files.list(getRootPath())
                .filter(Files::isDirectory)
                .map(Path::getFileName)
                .map(Path::toString);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                if (S3fileSystem != null) {
                    S3fileSystem.close();
                }
            } catch (Exception e) {
                throw new IOException("Failed to close S3 storage", e);
            }
        }
    }
}
