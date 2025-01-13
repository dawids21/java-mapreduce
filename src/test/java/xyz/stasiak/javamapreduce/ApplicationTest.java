package xyz.stasiak.javamapreduce;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import xyz.stasiak.javamapreduce.cli.Command;
import xyz.stasiak.javamapreduce.cli.CommandWithArguments;
import xyz.stasiak.javamapreduce.rmi.ProcessingStatus;
import xyz.stasiak.javamapreduce.rmi.RemoteServer;

class ApplicationTest {

    record TestFile(String name, String content) {
    }

    @TempDir
    Path tempDir;

    private Path inputDir;
    private Path outputDir;
    private RemoteServer remoteServer;

    @BeforeAll
    static void setUpLogging() throws IOException {
        LogManager.getLogManager().readConfiguration(
                ApplicationTest.class.getClassLoader().getResourceAsStream("logging.properties"));
    }

    @BeforeEach
    void setUp() throws IOException, NotBoundException {
        inputDir = tempDir.resolve("input");
        outputDir = tempDir.resolve("output");

        Files.createDirectory(inputDir);
        Files.createDirectory(outputDir);

        new Application();
        var port = Integer.parseInt(Application.getRmiPort());
        Registry rmiRegistry = LocateRegistry.getRegistry(port);
        remoteServer = (RemoteServer) rmiRegistry.lookup("server");
    }

    private void createTestFiles(List<TestFile> files) throws IOException {
        for (var file : files) {
            var path = inputDir.resolve(file.name);
            Files.writeString(path, file.content);
        }
    }

    @Test
    void shouldProcessWordCount() throws IOException, InterruptedException, NotBoundException {
        var testFiles = List.of(
                new TestFile("file1.txt", "hello world\nworld hello\nhello hello"),
                new TestFile("file2.txt", "mapreduce test\ntest mapreduce\nerlang"));
        createTestFiles(testFiles);

        var startCommand = createStartCommand();

        var processingId = remoteServer.startProcessing(startCommand.toProcessingParameters());

        var status = waitForCompletion(processingId);

        assertEquals(ProcessingStatus.FINISHED, status);
    }

    private CommandWithArguments createStartCommand() {
        return new CommandWithArguments(
                Command.START,
                List.of(inputDir.toString(), outputDir.toString(), TestMapper.class.getName(),
                        TestReducer.class.getName()),
                "start " + inputDir + " " + outputDir + " " + TestMapper.class.getName() + " "
                        + TestReducer.class.getName());
    }

    private ProcessingStatus waitForCompletion(int processingId) throws InterruptedException, IOException {
        var status = ProcessingStatus.NOT_STARTED;
        var attempts = 0;
        var maxAttempts = 10;

        while (status != ProcessingStatus.FINISHED && attempts < maxAttempts) {
            Thread.sleep(1000);
            status = remoteServer.getProcessingStatus(processingId);
            attempts++;
        }

        return status;
    }
}
