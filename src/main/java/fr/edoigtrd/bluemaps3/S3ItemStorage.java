package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.core.storage.ItemStorage;
import de.bluecolored.bluemap.core.storage.compression.CompressedInputStream;
import fr.edoigtrd.bluemaps3.utils.S3PutOutputStream;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3ItemStorage implements ItemStorage {

    S3Storage s3;
    String root;

    public S3ItemStorage(S3Storage s3, String root) {
        this.s3 = s3;
        this.root = root;

        try {
            if (!s3.blobStore.containerExists(s3.config.getBucketName())) {
                s3.blobStore.createContainerInLocation(null, s3.config.getPrefix());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create root path in S3 storage: " + s3.config.getPrefix(), e);
        }
    }

    AtomicBoolean isClosed = new AtomicBoolean(false);

    private String path() {
        return s3.config.getPrefix() + "/" + this.root;
    }

    @Override
    public OutputStream write() throws IOException {
        return new S3PutOutputStream(s3.blobStore, s3.config.getBucketName(), path(), "application/octet-stream");
    }

    @Override
    public @Nullable CompressedInputStream read() throws IOException {
        return null;
    }

    @Override
    public void delete() throws IOException {
        this.s3.blobStore.removeBlob(s3.config.getBucketName(), path());
    }

    @Override
    public boolean exists() throws IOException {

        return this.s3.blobStore.blobExists(s3.config.getBucketName(), path());
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }
}
