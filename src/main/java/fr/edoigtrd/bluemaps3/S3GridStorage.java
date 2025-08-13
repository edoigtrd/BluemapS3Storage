package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.core.storage.GridStorage;
import de.bluecolored.bluemap.core.storage.ItemStorage;
import de.bluecolored.bluemap.core.storage.compression.CompressedInputStream;
import fr.edoigtrd.bluemaps3.utils.S3PutOutputStream;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

public class S3GridStorage implements GridStorage {
    S3Storage s3;
    String root;

    AtomicBoolean isClosed = new AtomicBoolean(false);

    public S3GridStorage (S3Storage s3, String root) {
        this.s3 = s3;
        this.root = root;

        try {
            if (!s3.blobStore.containerExists(s3.config.getBucketName())) {
                s3.blobStore.createContainerInLocation(null, root);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create root path in S3 storage: " + root, e);
        }
    }

    @Override
    public OutputStream write(int i, int i1) throws IOException {
        String path = String.format("%s/%s/x%d-z%d",s3.config.getPrefix(), root, i, i1);

        return new S3PutOutputStream(s3.blobStore, s3.config.getBucketName()
                , path, "application/octet-stream");
    }

    @Override
    public @Nullable CompressedInputStream read(int i, int i1) throws IOException {
        return null;
    }

    @Override
    public void delete(int i, int i1) throws IOException {
        String path = String.format("%s/%s/x%d-z%d", s3.config.getPrefix(), root, i, i1);

        this.s3.blobStore.removeBlob(
            this.s3.config.getBucketName(),
            path
        );
    }

    @Override
    public boolean exists(int i, int i1) throws IOException {
        String path = String.format("%s/%s/x%d-z%d", s3.config.getPrefix(), root, i, i1);
        return this.s3.blobStore.blobExists(this.s3.config.getBucketName(), path);
    }

    @Override
    public ItemStorage cell(int i, int i1) {
        return new S3ItemStorage(s3, String.format("%s/%s/x%d-z%d/cell.json", s3.config.getPrefix(), root, i, i1));
    }

    @Override
    public Stream<Cell> stream() throws IOException {
        ListContainerOptions options = ListContainerOptions.Builder
                .prefix(s3.config.getPrefix() + "/" + root + "/x")
                .recursive();
        return s3.blobStore.list(s3.config.getBucketName(), options)
                .stream()
                .filter(blob -> blob.getName().startsWith(s3.config.getPrefix() + "/" + root + "/x"))
                .map(blob -> {
                    String[] parts = blob.getName().split("/");
                    int x = Integer.parseInt(parts[parts.length - 2].substring(1));
                    int y = Integer.parseInt(parts[parts.length - 1].substring(1));
                    return new S3Cell(this.s3 ,x, y);
                });
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }
}
