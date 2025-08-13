package fr.edoigtrd.bluemaps3.utils;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;

import java.io.*;

public final class S3PutOutputStream extends OutputStream {
    private final BlobStore store;
    private final String bucket;
    private final String key;
    private final String contentType;
    private final File temp;
    private final OutputStream out;

    public S3PutOutputStream(BlobStore store, String bucket, String key, String contentType) throws IOException {
        this.store = store;
        this.bucket = bucket;
        this.key = key;
        this.contentType = contentType;
        this.temp = File.createTempFile("s3put-", ".tmp");
        this.out = new BufferedOutputStream(new FileOutputStream(temp));
    }

    @Override public void write(int b) throws IOException { out.write(b); }
    @Override public void write(byte[] b, int off, int len) throws IOException { out.write(b, off, len); }
    @Override public void flush() throws IOException { out.flush(); }

    @Override public void close() throws IOException {
        IOException ioex = null;
        try { out.close(); } catch (IOException e) { ioex = e; }
        try {
            Blob blob = store.blobBuilder(key)
                    .payload(temp)
                    .contentLength(temp.length())
                    .contentType(contentType)
                    .build();
            store.putBlob(bucket, blob);
        } finally {
            // best-effort cleanup
            try { temp.delete(); } catch (Exception ignored) {}
        }
        if (ioex != null) throw ioex;
    }
}
