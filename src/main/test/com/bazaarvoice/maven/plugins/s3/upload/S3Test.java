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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class S3Test extends Assert {

    private AmazonS3Client client;
    private String bucket;
    private String diretorio;
    private MimetypesFileTypeMap typeMap = new MimetypesFileTypeMap();
    private Logger log = LoggerFactory.getLogger(S3Test.class);
    private ExecutorService executor;
    private int indice = 0;

    private LinkedList<com.bazaarvoice.maven.plugins.s3.upload.Permission> permissions = new LinkedList<com.bazaarvoice.maven.plugins.s3.upload.Permission>();

    private LinkedList<Metadata> metadatas = new LinkedList<Metadata>();

    @Before
    public void init() {
        client = new AmazonS3Client(new BasicAWSCredentials("AKIAI23EYS6CRTNPM3YA", "sua-chave"));
        client.setEndpoint("https://s3-sa-east-1.amazonaws.com");
        bucket = "testebucket1";

        executor = Executors.newFixedThreadPool(30);
        metadatas.add(new Metadata("Cache-Control", "max-age=31536000", null));
        metadatas.add(new Metadata("Content-Type", "text/css", ".*(css|css\\.gz)$"));
        metadatas.add(new Metadata("Content-Type", "application/javascript", ".*(js|js\\.gz)$"));

        permissions.add(new com.bazaarvoice.maven.plugins.s3.upload.Permission());
        permissions.getFirst().setGrantee("Everyone");
        permissions.getFirst().setDownload(true);
    }

    @Test
    public void deveListarDiretorios() throws Exception {
        final File root = new File(getClass().getResource("/assets").toURI());
        assertNotNull(root);
        assertTrue(root.isDirectory());

        final List<String> filePaths = listFiles(root, new LinkedList<String>());
        System.out.println(filePaths.toString().replace(" ", "\n"));

        for (final String filePath : filePaths) {
            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    InputStream is = null;
                    String key = null;
                    try {
                        File file = new File(filePath);
                        is = new FileInputStream(file);
                        assertNotNull(is);

                        key = getFileKey(filePath, root.getAbsolutePath(), "assets");
                        ObjectMetadata om = getObjectMetaData(key, file);

                        PutObjectRequest por = new PutObjectRequest(bucket, key, is, om);
                        por.setCannedAcl(CannedAccessControlList.PublicRead);

                        // client.putObject(bucket, key, is, om);
                        client.putObject(por);
                        AccessControlList acl = client.getObjectAcl(bucket,
                            key);
                        setPermission(acl);

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        try {
                            is.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    log.info("{} de {} arquivos, {}% concluido. Arquivo {}",
                        new Object[] { ++indice, filePaths.size(), (indice * 100 / filePaths.size()), key });
                }

            };
            executor.execute(runnable);

        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);

    }

    protected void setPermission(AccessControlList acl) {
        for (com.bazaarvoice.maven.plugins.s3.upload.Permission permission : permissions) {
            acl.grantPermission(permission.getAsGrantee(), permission.getPermission());
        }
    }

    protected ObjectMetadata getObjectMetaData(String key, File file) {
        ObjectMetadata om = new ObjectMetadata();
        for (Metadata metadata : metadatas) {
            if (metadata.shouldSetMetadata(key)) {
                om.setHeader(metadata.getKey(), metadata.getValue());
            }
        }
        om.setHeader("Content-Length", file.length());
        return om;
    }

    protected String getFileKey(String filePath, String rootFilePath, String source) {
        return filePath.replace(rootFilePath.replace(source, ""), "").replace("\\", "/");
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

    @Test
    @Ignore
    public void deveCopiarArquivo() {
        InputStream is = getClass().getResourceAsStream("/assets/teste.js");
        assertNotNull(is);
        String key = "pasta2\teste2.js";
        ObjectMetadata om = new ObjectMetadata();
        om.setHeader("Cache-Control", "max-age=31536000");
        om.setHeader("Content-Type", "application/javascript");

        client.putObject("testebucket1", key, is, om);
        AccessControlList acl = client.getObjectAcl("testebucket1", key);
        acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
        client.setObjectAcl("testebucket1", key, acl);
    }

    @Test
    @Ignore
    public void adicionarCacheTag() throws InterruptedException {
        ObjectListing objetos = client.listObjects(bucket, diretorio);
        final List<S3ObjectSummary> lista = objetos.getObjectSummaries();
        while (objetos.isTruncated()) {
            objetos = client.listNextBatchOfObjects(objetos);
            lista.addAll(objetos.getObjectSummaries());
        }

        for (final S3ObjectSummary item : lista) {

            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    S3Object obj = client.getObject(item.getBucketName(), item.getKey());

                    ObjectMetadata metadata = new ObjectMetadata();
                    metadata.setContentType(typeMap.getContentType(item.getKey()));
                    metadata.setCacheControl("max-age=31536000");
                    metadata.setContentLength(obj.getObjectMetadata().getContentLength());

                    obj.setObjectMetadata(metadata);
                    client.putObject(item.getBucketName(), item.getKey(), obj.getObjectContent(), obj.getObjectMetadata());

                    // log.info("Configurando permissão para {}",
                    // item.getKey());
                    AccessControlList acl = client.getObjectAcl(item.getBucketName(), item.getKey());
                    acl.grantPermission(GroupGrantee.AllUsers, com.amazonaws.services.s3.model.Permission.Read);
                    client.setObjectAcl(item.getBucketName(), item.getKey(), acl);

                    log.info("{} de {} arquivos, {}% concluido. Arquivo {}/{}",
                        new Object[] { ++indice, lista.size(), (indice * 100 / lista.size()), item.getBucketName(), item.getKey() });

                }
            };

            executor.execute(runnable);

        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);
    }

    @Test
    @Ignore
    public void copiar() throws InterruptedException {
        ObjectListing objetos = client.listObjects(bucket, diretorio);
        final List<S3ObjectSummary> lista = objetos.getObjectSummaries();
        // final String bucketDiretorioDestino =
        // "cdn.delogic.com.br/teleguiasalto";
        final String bucketDiretorioDestino = "bucket destino";
        while (objetos.isTruncated()) {
            objetos = client.listNextBatchOfObjects(objetos);
            lista.addAll(objetos.getObjectSummaries());
        }

        for (final S3ObjectSummary item : lista) {

            Runnable runnable = new Runnable() {

                @Override
                public void run() {
                    // copiar
                    S3Object obj = client.getObject(item.getBucketName(), item.getKey());
                    client.putObject(bucketDiretorioDestino, item.getKey(), obj.getObjectContent(), obj.getObjectMetadata());

                    // dar permissão de leitura para todos
                    AccessControlList acl = client.getObjectAcl(item.getBucketName(), item.getKey());
                    acl.grantPermission(GroupGrantee.AllUsers, com.amazonaws.services.s3.model.Permission.Read);
                    client.setObjectAcl(bucketDiretorioDestino, item.getKey(), acl);

                    log.info("{} de {} arquivos, {}% concluido. Arquivo {}/{}",
                        new Object[] { ++indice, lista.size(), (indice * 100 / lista.size()), item.getBucketName(), item.getKey() });

                }
            };

            executor.execute(runnable);

        }
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.MINUTES);
    }

}
