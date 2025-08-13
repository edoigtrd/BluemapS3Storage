package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.core.storage.GridStorage;
import de.bluecolored.bluemap.core.storage.ItemStorage;
import de.bluecolored.bluemap.core.storage.MapStorage;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.DoublePredicate;

public class S3MapStorage implements MapStorage {
    S3Storage s3;

    public S3MapStorage(S3Storage s3) {
        this.s3 = s3;
    }

    @Override
    public GridStorage hiresTiles() {
        return new S3GridStorage(s3, "hires");
    }

    @Override
    public GridStorage lowresTiles(int i) {
        return new S3GridStorage(s3, "lowres" + i);
    }

    @Override
    public GridStorage tileState() {
        return new S3GridStorage(s3, "tileState");
    }

    @Override
    public GridStorage chunkState() {
        return new S3GridStorage(s3, "chunkState");
    }

    @Override
    public ItemStorage asset(String s) {
        return new S3ItemStorage(s3, "assets");
    }

    @Override
    public ItemStorage settings() {
        return new S3ItemStorage(s3, "settings");
    }

    @Override
    public ItemStorage textures() {
        return new S3ItemStorage(s3, "textures");
    }

    @Override
    public ItemStorage markers() {
        return new S3ItemStorage(s3, "markers");
    }

    @Override
    public ItemStorage players() {
        return new S3ItemStorage(s3, "players");
    }

    @Override
    public void delete(DoublePredicate onProgress) throws IOException {
        // 1) collect keys to support progress and cancellation
        // 2) delete keys from the end to mirror your local LinkedList pattern
        // 3) remove possible "directory placeholder" object (zero-byte "<prefix>/")

        ListContainerOptions options = ListContainerOptions.Builder.prefix(s3.config.getPrefix()).recursive();
        PageSet<? extends StorageMetadata> blobs = s3.blobStore.list(s3.config
                .getBucketName(), options);
        LinkedList<String> keys = new LinkedList<>();
        for (StorageMetadata blob : blobs) {
            if (blob.getName().endsWith("/")) continue; // skip "directory" placeholders
            keys.add(blob.getName());
        }
        int total = keys.size();
        int count = 0;
        for (Iterator<String> it = keys.descendingIterator(); it.hasNext(); ) {
            String key = it.next();
            if (onProgress.test((double) count / total)) {
                s3.blobStore.removeBlob(s3.config.getBucketName(), key);
                count++;
            } else {
                throw new IOException("Deletion cancelled by user.");
            }

        }
        // remove the "directory placeholder" if it exists
        String rootPath = s3.config.getPrefix() + "/";
        if (s3.blobStore.blobExists(s3.config.getBucketName(), rootPath)) {
            s3.blobStore.removeBlob(s3.config.getBucketName(), rootPath);
        }
    }

    @Override
    public boolean exists() throws IOException {
        ListContainerOptions options = ListContainerOptions.Builder.prefix(s3.config.getPrefix()).maxResults(1);
        PageSet<? extends StorageMetadata> blobs = s3.blobStore.list(s3.config.getBucketName(), options);
        return !blobs.isEmpty();
    }

    @Override
    public boolean isClosed() {
        return s3.isClosed();
    }
}
