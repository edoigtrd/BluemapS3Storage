# BlueMap S3

This Bluemap addon allows you to upload your Bluemap maps to an S3 bucket.

# Installation

1. put the BlueMapS3.jar into your Bluemap addons folder. (plugins/BlueMap/packs)
2. Copy this into a new file in (plugins/BlueMap/storages/s3.conf)
``` 
storage-type: "edoigtrd:s3"

access-key: ""
secret-key: ""
bucket-endpoint: "https://xxxxx"
bucket-name: 
bucket-region: f

# Prefix is the name of the bluemap root folder in the bucket
# It's usefull for when your bucket stores multiple bluemap instances
# If you don't know what to put in it, set as "bluemap" or your server's name
prefix: myserver
```

3. In .plugins/BlueMap/world.conf, set the storage to s3
```
storage: "edoigtrd:s3"
```
It should be around line 141, do it for all worlds you want to upload to S3.

# Development

If you want to contribute, feel free to open a pull request or an issue.
Make sure you are using java-21 not higher nor lower.