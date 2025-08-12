package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.common.config.ConfigurationException;

import de.bluecolored.bluemap.common.config.storage.StorageConfig;
import de.bluecolored.bluemap.core.storage.Storage;
import org.spongepowered.configurate.objectmapping.ConfigSerializable;
import org.spongepowered.configurate.objectmapping.meta.Setting;


@ConfigSerializable
public class S3Config extends StorageConfig implements IS3Configuration {
    @Setting("access-key")
    private String accessKey;

    @Setting("secret-key")
    private String secretKey;

    @Setting("bucket-endpoint")
    private String endpoint;

    @Setting("bucket-name")
    private String bucketName;

    @Setting("bucket-region")
    private String bucketRegion;

    @Setting("prefix")
    private String prefix;

    @Override
    public Storage createStorage() throws ConfigurationException {
        return new S3Storage(this);
    }

    @Override
    public String getAccessKey() {
        return accessKey;
    }

    @Override
    public String getSecretKey() {
        return  secretKey;
    }

    @Override
    public String getEndpoint() {
        return endpoint;
    }

    @Override
    public String getBucketRegion() {
        return  bucketRegion;
    }

    @Override
    public String getPrefix() {
        return  prefix;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }
}
