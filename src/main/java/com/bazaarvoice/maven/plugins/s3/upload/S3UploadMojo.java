package com.bazaarvoice.maven.plugins.s3.upload;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.activation.MimetypesFileTypeMap;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

@Mojo(name = "s3-upload")
public class S3UploadMojo extends AbstractMojo {
    /** Access key for S3. */
    @Parameter(property = "s3-upload.accessKey")
    private String accessKey;

    /** Secret key for S3. */
    @Parameter(property = "s3-upload.secretKey")
    private String secretKey;

    /**
     * Execute all steps up except the upload to the S3. This can be set to true
     * to perform a "dryRun" execution.
     */
    @Parameter(property = "s3-upload.doNotUpload", defaultValue = "false")
    private boolean doNotUpload;

    /**
     * Skips execution
     */
    @Parameter(property = "s3-upload.skip", defaultValue = "false")
    private boolean skip;

    /** The file/folder to upload. */
    @Parameter(property = "s3-upload.source", required = true)
    private String source;

    /** The bucket to upload into. */
    @Parameter(property = "s3-upload.bucketName", required = true)
    private String bucketName;

    /** The file/folder (in the bucket) to create. */
    @Parameter(property = "s3-upload.destination", required = true)
    private String destination;

    /** Force override of endpoint for S3 regions such as EU. */
    @Parameter(property = "s3-upload.endpoint")
    private String endpoint;

    /** In the case of a directory upload, recursively upload the contents. */
    @Parameter(property = "s3-upload.recursive", defaultValue = "false")
    private boolean recursive;

    @Parameter(property = "s3-upload.permissions")
    private LinkedList<Permission> permissions;

    @Parameter(property = "s3-upload.metadatas")
    private LinkedList<Metadata> metadatas;

    private int indice = 0;
    
    private MimetypesFileTypeMap mimeTypes;

    @Override
    public void execute() throws MojoExecutionException {
        indice = 0;
        mimeTypes = new MimetypesFileTypeMap(getClass().getResourceAsStream("/META-INF/mime.types"));
        
        if (skip) {
            getLog().info("Skipping S3UPload");
            return;
        }
        File sourceFile = new File(source);
        if (!sourceFile.exists()) {
            throw new MojoExecutionException("File/folder doesn't exist: " + source);
        }

        AmazonS3 s3 = getS3Client(accessKey, secretKey);
        if (endpoint != null) {
            s3.setEndpoint(endpoint);
        }

        if (!s3.doesBucketExist(bucketName)) {
            throw new MojoExecutionException("Bucket doesn't exist: " + bucketName);
        }

        if (doNotUpload) {
            getLog().info(String.format("File %s would have be uploaded to s3://%s/%s (dry run)", sourceFile, bucketName, destination));

            return;
        }

        try {
            upload(s3, sourceFile);
        } catch (Exception e) {
            throw new MojoExecutionException("Unable to upload file to S3", e);
        }

        getLog().info(String.format("File %s uploaded to s3://%s/%s", sourceFile, bucketName, destination));
    }

    private static AmazonS3 getS3Client(String accessKey, String secretKey) {
        AWSCredentialsProvider provider;
        if (accessKey != null && secretKey != null) {
            AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
            provider = new StaticCredentialsProvider(credentials);
        } else {
            provider = new DefaultAWSCredentialsProviderChain();
        }

        return new AmazonS3Client(provider);
    }

    private void upload(final AmazonS3 s3, final File root) throws MojoExecutionException, Exception {

        final List<String> filePaths = listFiles(root, new LinkedList<String>());
        if (getLog().isDebugEnabled()) {
            getLog().debug("Files to upload:" + filePaths.toString().replace(" ", "\n"));
        }

        ExecutorService executor = Executors.newFixedThreadPool(20);

        for (final String filePath : filePaths) {

            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    InputStream is = null;
                    String key = null;
                    try {
                        File file = new File(filePath);
                        is = new FileInputStream(file);
                        key = getFileKey(filePath, root.getAbsolutePath(), source, destination);
                        ObjectMetadata om = getObjectMetaData(key, file);

                        PutObjectRequest por = new PutObjectRequest(bucketName, key, is, om);
                        por.setCannedAcl(CannedAccessControlList.PublicRead);

                        // client.putObject(bucket, key, is, om);
                        s3.putObject(por);

                    } catch (Exception e) {
                        getLog().error(e);
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            is.close();
                        } catch (IOException e) {
                            getLog().error(e);
                            throw new RuntimeException(e);
                        }
                    }
                    getLog().info(String.format("%s de %s arquivos, %s porcento concluido. Arquivo %s", ++indice, filePaths.size(),
                        (indice * 100 / filePaths.size()), key));
                }

            };
            executor.execute(runnable);

        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);

    }

    protected ObjectMetadata getObjectMetaData(String key, File file) {
        ObjectMetadata om = new ObjectMetadata();
        for (Metadata metadata : metadatas) {
            if (metadata.shouldSetMetadata(key)) {
                om.setHeader(metadata.getKey(), metadata.getValue());
            }
        }
        om.setHeader("Content-Length", file.length());
        
        if (!om.getRawMetadata().containsKey(Headers.CONTENT_TYPE)){
            om.setHeader(Headers.CONTENT_TYPE, mimeTypes.getContentType(file));
        }
        
        return om;
    }

    protected static String getFileKey(String filePath, String rootFilePath, String source, String destination) {
        return (destination + filePath.replace(rootFilePath, "")).replace("\\", "/");
    }

    private List<String> listFiles(File root, List<String> paths) throws Exception {
        if (root.isDirectory()) {
            for (File innerFile : root.listFiles()) {
                listFiles(innerFile, paths);
            }
        } else {
            paths.add(root.getAbsolutePath());
        }
        return paths;
    }

}
