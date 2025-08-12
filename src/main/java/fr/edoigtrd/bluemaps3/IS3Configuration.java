package fr.edoigtrd.bluemaps3;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public interface IS3Configuration {
    public String getAccessKey();
    public String getSecretKey();
    public String getEndpoint();
    public String getBucketRegion();
    public String getPrefix();                 // prefix
    public String getBucketName();
}
