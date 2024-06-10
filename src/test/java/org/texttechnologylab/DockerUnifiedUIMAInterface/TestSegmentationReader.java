package org.texttechnologylab.DockerUnifiedUIMAInterface;

import com.google.common.collect.Lists;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.uima.util.InvalidXMLException;
import org.dkpro.core.io.xmi.XmiWriter;
import org.junit.jupiter.api.Test;
import org.texttechnologylab.DockerUnifiedUIMAInterface.connection.mongodb.MongoDBConfig;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.*;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.AsyncCollectionReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.reader.DUUISegmentationReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.lua.DUUILuaContext;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.sqlite.DUUISqliteStorageBackend;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategy;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyByDelemiter;
import org.texttechnologylab.DockerUnifiedUIMAInterface.segmentation.DUUISegmentationStrategyBySentence;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

public class TestSegmentationReader {

    private enum MODEL {
        SENTIMENT,
    }

    private enum TASK {
        DEFAULT,
        SEGMENTED_RAW,
        SEGMENTED_STRUCT,
        SEGMENTED_STRUCT_MONGO
    }

    @Test
    public void testAll() throws Exception {
        //List<Integer> delimiterRange = List.of(20, 50, 100, 200, 500, 1000, 5000, 10_000);
        String corpus = "gerparcor_sample_1000_RANDOM_100_SENT";
        testDuui(corpus, MODEL.SENTIMENT);
        System.gc();
        testSegmented(corpus, 1000, TASK.SEGMENTED_RAW, MODEL.SENTIMENT);
        System.gc();
        testSegmented(corpus, 30, TASK.SEGMENTED_STRUCT, MODEL.SENTIMENT);
        System.gc();
        testSegmentedMongo(corpus, 30, TASK.SEGMENTED_STRUCT_MONGO, MODEL.SENTIMENT);

    }

    public void testDuui(String corpus, MODEL model) throws Exception {
        int toolWorkers = 1;

        Path out = Paths.get("/opt/sample/out/" + corpus + "_" + "duui" + "_" + model.name());
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("./benchmark.db")
                .withConnectionPoolSize(toolWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers)
                .withStorageBackend(sqlite);

        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();

        composer.addDriver(swarmDriver);
        composer.addDriver(remoteDriver);
        composer.addDriver(dockerDriver);
        composer.addDriver(uimaDriver);

        AsyncCollectionReader reader = new AsyncCollectionReader(
                "/opt/sample/" + corpus,
                "xmi.gz",
                1,
                -1,
                false,
                "",
                false,
                null
        );


        addModel(composer, toolWorkers, model);

        DUUIUIMADriver.Component writer = new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class
                , XmiWriter.PARAM_TARGET_LOCATION, out.toString()
                , XmiWriter.PARAM_PRETTY_PRINT, true
                , XmiWriter.PARAM_OVERWRITE, false
                , XmiWriter.PARAM_VERSION, "1.1"
                , XmiWriter.PARAM_COMPRESSION, "GZIP"
        ));
        composer.add(writer);

        composer.run(reader, corpus + "_" + "duui" + "_" + model.name());
        composer.shutdown();
    }

    public void testSegmented(String corpus, int delimeterSize, TASK task, MODEL model) throws Exception {
        int toolWorkers = 1;

        Path out = Paths.get("/opt/sample/out/" + corpus + "_" + delimeterSize + "_" + task.name() + "_" + model.name());

        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("./benchmark.db")
                .withConnectionPoolSize(toolWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers)
                .withStorageBackend(sqlite);

        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();

        composer.addDriver(swarmDriver);
        composer.addDriver(remoteDriver);
        composer.addDriver(dockerDriver);
        composer.addDriver(uimaDriver);

        DUUISegmentationStrategy segmentationStrategy;
        switch (task) {
            case SEGMENTED_RAW:
                segmentationStrategy = new DUUISegmentationStrategyByDelemiter()
                        .withDelemiter(".")
                        .withLength(delimeterSize);
                break;
            case SEGMENTED_STRUCT:
            default:
                segmentationStrategy = new DUUISegmentationStrategyBySentence()
                        .withMaxAnnotationsPerSegment(delimeterSize)
                        .withMaxCharsPerSegment(1000000);

        }

        AsyncCollectionReader reader = new AsyncCollectionReader(
                "/opt/sample/" + corpus,
                "xmi.gz",
                1,
                -1,
                false,
                "",
                false,
                null
        );


        addModel(composer, toolWorkers, model, segmentationStrategy);

        DUUIUIMADriver.Component writer = new DUUIUIMADriver.Component(createEngineDescription(XmiWriter.class
                , XmiWriter.PARAM_TARGET_LOCATION, out.toString()
                , XmiWriter.PARAM_PRETTY_PRINT, true
                , XmiWriter.PARAM_OVERWRITE, false
                , XmiWriter.PARAM_VERSION, "1.1"
                , XmiWriter.PARAM_COMPRESSION, "GZIP"
        ));
        composer.add(writer);

        composer.run(reader, corpus + "_" + delimeterSize + "_" + task.name() + "_" + model.name());
        composer.shutdown();
    }

    public void testSegmentedMongo(String corpus, int delimeterSize, TASK task, MODEL model) throws Exception {
        int toolWorkers = 1;

        int segmentationWorkers = 1;
        int segmentationQueueSize = Integer.MAX_VALUE;

        Path out = Paths.get("/opt/sample/out/" + corpus + "_" + delimeterSize + "_" + task.name() + "_" + model.name());
        if (!Files.exists(out))
            Files.createDirectory(out);
        DUUISqliteStorageBackend sqlite = new DUUISqliteStorageBackend("./benchmark.db")
                .withConnectionPoolSize(toolWorkers);

        DUUIComposer composer = new DUUIComposer()
                .withSkipVerification(true)
                .withLuaContext(new DUUILuaContext().withJsonLibrary())
                .withWorkers(toolWorkers)
                .withStorageBackend(sqlite);

        DUUISwarmDriver swarmDriver = new DUUISwarmDriver();
        DUUIRemoteDriver remoteDriver = new DUUIRemoteDriver();
        DUUIDockerDriver dockerDriver = new DUUIDockerDriver();
        DUUIUIMADriver uimaDriver = new DUUIUIMADriver();

        composer.addDriver(swarmDriver);
        composer.addDriver(remoteDriver);
        composer.addDriver(dockerDriver);
        composer.addDriver(uimaDriver);

        MongoDBConfig mongoConfig = new MongoDBConfig("segmentation_mongo.properties");

        DUUISegmentationStrategy segmentationStrategy = new DUUISegmentationStrategyBySentence()
                .withMaxAnnotationsPerSegment(delimeterSize)
                .withMaxCharsPerSegment(1000000);

        DUUISegmentationReader reader = new DUUISegmentationReader(
                Paths.get("/opt/sample/" + corpus),
                out,
                mongoConfig,
                segmentationStrategy,
                segmentationWorkers,
                segmentationQueueSize
        );

        System.out.println("Size: " + reader.getSize());
        System.out.println("Done: " + reader.getDone());
        System.out.println("Progress: " + reader.getProgress());

        addModel(composer, toolWorkers, model);

        composer.runSegmented(reader, corpus + "_" + delimeterSize + "_" + task.name() + "_" + model.name());
        composer.shutdown();
    }

    private void addModel(DUUIComposer composer, int toolWorkers, MODEL model) throws CompressorException, InvalidXMLException, URISyntaxException, IOException, SAXException {
        addModel(composer, toolWorkers, model, null);
    }

    private void addModel(DUUIComposer composer, int toolWorkers, MODEL model, DUUISegmentationStrategy strategy) throws URISyntaxException, IOException, CompressorException, InvalidXMLException, SAXException {
        DUUIDockerDriver.Component driver;
        switch (model) {
            case SENTIMENT:
            default:
                driver = new DUUIDockerDriver
                        .Component("docker.texttechnologylab.org/textimager-duui-transformers-sentiment:0.1.3")
                        .withScale(toolWorkers)
                        .withParameter("model_name", "oliverguhr/german-sentiment-bert")
                        .withParameter("selection", "text")
                        .withParameter("ignore_max_length_truncation_padding", String.valueOf(true));
                if (strategy != null)
                    driver.withSegmentationStrategy(strategy);
                break;
        }
        composer.add(driver);
    }

}
