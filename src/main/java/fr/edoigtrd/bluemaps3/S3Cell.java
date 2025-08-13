package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.core.storage.GridStorage;
import de.bluecolored.bluemap.core.storage.compression.CompressedInputStream;
import fr.edoigtrd.bluemaps3.utils.S3PutOutputStream;
import org.jclouds.blobstore.BlobStore;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public class S3Cell implements GridStorage.Cell {
    S3Storage s3;
    int x;
    int z;
    AtomicBoolean isClosed = new AtomicBoolean(false);

    public S3Cell(S3Storage s3, int x, int z) {
        this.s3 = s3;
        this.x = x;
        this.z = z;

        try {
            BlobStore blobStore = s3.blobStore;
            String bucketName = s3.config.getBucketName();
            String prefix = s3.config.getPrefix();

            if (!blobStore.containerExists(bucketName)) {
                blobStore.createContainerInLocation(null, prefix);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create root path in S3 storage: " + s3.config.getPrefix(), e);
        }
    }

    @Override
    public int getX() {
        return x;
    }

    @Override
    public int getZ() {
        return z;
    }

    @Override
    public OutputStream write() throws IOException {
        String path = String.format("%s/cell/x%d-z%d", s3.config.getPrefix(), getX(), getZ());
        return new S3PutOutputStream(s3.blobStore, s3.config.getBucketName(), path, "application/octet-stream");
    }

    @Override
    public @Nullable CompressedInputStream read() throws IOException {
        return null;
    }

    @Override
    public void delete() throws IOException {
        s3.blobStore.removeBlob(s3.config.getBucketName(), String.format("%s/cell/x%d-z%d", s3.config.getPrefix(), getX(), getZ()));
    }

    @Override
    public boolean exists() throws IOException {
        return s3.blobStore.blobExists(s3.config.getBucketName(), String.format("%s/cell/x%d-z%d", s3.config.getPrefix(), getX(), getZ()));
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }
}
