package fr.edoigtrd.bluemaps3;

import de.bluecolored.bluemap.common.config.storage.StorageConfig;
import de.bluecolored.bluemap.common.config.storage.StorageType;
import de.bluecolored.bluemap.core.util.Key;


public class BluemapAddon implements Runnable {
    @Override
    public void run() {
        StorageType.REGISTRY.register(new S3StorageType());
    }

    class S3StorageType implements StorageType {
        private final Key key = new Key("edoigtrd", "s3");

        private final Class<? extends StorageConfig> configType = S3Config.class;

        @Override
        public Key getKey() {
            return key;
        }

        @Override
        public Class<? extends StorageConfig> getConfigType() {
            return configType;
        }
    }
}
