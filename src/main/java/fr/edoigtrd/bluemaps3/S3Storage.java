package fr.edoigtrd.bluemaps3;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import de.bluecolored.bluemap.core.storage.Storage;
import de.bluecolored.bluemap.core.storage.compression.Compression;
import de.bluecolored.bluemap.core.storage.file.FileMapStorage;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class S3Storage implements Storage {
    private IS3Configuration config;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private FileSystem S3fileSystem;
    //private final LoadingCache<String, FileMapStorage> mapStorages;

    public FileMapStorage create(String mapId) {
        Path mapPath = getRootPath().resolve(mapId);
        return new FileMapStorage(mapPath, Compression.NONE, false);
    }


    public S3Storage(IS3Configuration config) {
            this.config = config;
            //mapStorages = Caffeine.newBuilder().build(this::create);
    }

    private Path getRootPath() {
        return S3fileSystem.getPath(config.getPrefix());
    }

    @Override
    public void initialize() throws IOException {
        Map<String, String> env = new HashMap<>();
        env.put("jclouds.provider", "aws-s3");
        env.put("jcoulds.identity",this.config.getAccessKey());
        env.put("jcoulds.credentials",this.config.getSecretKey());
        env.put("jcoulds.endpoint",this.config.getEndpoint());
        env.put("jclouds.s3.virtual-host-buckets", "false");

        URI uri = URI.create(
                String.format("jclouds:s3://%s", config.getBucketName())
        );

        this.S3fileSystem = FileSystems.newFileSystem(uri, env);
    }

    @Override
    public FileMapStorage map(String s) {
        //return this.mapStorages.get(s);
        return create(s);
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
