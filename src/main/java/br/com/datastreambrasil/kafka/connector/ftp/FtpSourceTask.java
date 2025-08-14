package br.com.datastreambrasil.kafka.connector.ftp;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

public class FtpSourceTask extends SourceTask {

    public static final String VERSION = FtpSourceConnector.VERSION;
    private static final Logger log = LoggerFactory.getLogger(FtpSourceTask.class);

    protected RemoteClient client;
    private String topic;
    private String directory;
    private String stageDir;
    private String archiveDir;
    private String filePattern;
    private String fileEncoding;
    protected String outputFormat;
    protected String tokenizer;
    private String keyFieldName;
    private long pollInterval;
    private int maxRecordsPerPoll;

    private BufferedReader currentReader;
    private InputStream currentStream;
    private String currentFilename;
    private String currentStagedPath;
    private long linesProcessed;
    protected List<String> fieldHeaders;

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting FtpSourceTask");

        String protocol = props.get(FtpSourceConnector.FTP_PROTOCOL);
        this.topic = props.get(FtpSourceConnector.TOPIC);
        this.directory = props.get(FtpSourceConnector.FTP_DIRECTORY);
        this.stageDir = props.get(FtpSourceConnector.FTP_DIRECTORY_STAGE);
        this.archiveDir = props.get(FtpSourceConnector.FTP_DIRECTORY_ARCHIVE);
        this.filePattern = props.get(FtpSourceConnector.FTP_FILE_PATTERN);
        this.fileEncoding = props.getOrDefault(FtpSourceConnector.FTP_FILE_ENCODING, "UTF-8");
        this.outputFormat = props.getOrDefault(FtpSourceConnector.FTP_FILE_OUTPUT_FORMAT, "string").toLowerCase();
        this.tokenizer = props.getOrDefault(FtpSourceConnector.FTP_FILE_TOKENIZER, ";");
        this.keyFieldName = props.getOrDefault(FtpSourceConnector.FTP_KAFKA_KEY_FIELD, "").trim();
        this.pollInterval = Long.parseLong(props.getOrDefault(FtpSourceConnector.FTP_POLL_INTERVAL, "10000"));
        this.maxRecordsPerPoll = Integer.parseInt(
                props.getOrDefault(FtpSourceConnector.FTP_MAX_RECORDS_PER_POLL, "1000"));

        String headersConfig = props.getOrDefault(FtpSourceConnector.FTP_FILE_HEADERS, "").trim();
        this.fieldHeaders = headersConfig.isEmpty()
                ? null
                : Arrays.asList(headersConfig.split("\\s*,\\s*"));

        try {
            if (this.client == null) {
                this.client = "sftp".equalsIgnoreCase(protocol)
                        ? new SftpRemoteClient(props)
                        : new FtpRemoteClient(props);
            }
            this.client.connect();
            log.info("Connected to {} server", protocol.toUpperCase());

            this.currentReader = null;
            this.currentStream = null;
            this.currentFilename = null;
            this.currentStagedPath = null;
            this.linesProcessed = 0;
        } catch (Exception e) {
            log.error("Failed to connect to remote server", e);
            throw new ConnectException("Failed to connect to remote server", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();

        try {
            if (currentReader == null) {
                log.info("Polling files from directory: {}", directory);
                List<String> files = client.listFiles(directory, filePattern);
                if (files.isEmpty()) {
                    Thread.sleep(pollInterval);
                    return records;
                }

                String file = files.get(0);
                currentFilename = file.substring(file.lastIndexOf('/') + 1);
                currentStagedPath = stageDir + "/" + currentFilename;

                log.info("Staging file: {} → {}", file, currentStagedPath);
                client.moveFile(file, currentStagedPath);

                currentStream = client.retrieveFileStream(currentStagedPath);
                currentReader = new BufferedReader(new InputStreamReader(currentStream, fileEncoding));
                linesProcessed = 0;
            }

            boolean eof = false;
            String line = null;
            while (records.size() < maxRecordsPerPoll && (line = currentReader.readLine()) != null) {
                Map<String, String> sourcePartition = Collections.singletonMap("file", currentFilename);
                Map<String, Long> sourceOffset = Collections.singletonMap("position", System.currentTimeMillis());

                RecordModel record = buildRecordModel(line);
                Object value = record.value;
                Schema schema = record.schema;

                Object recordKey = null;
                if ("json".equals(outputFormat) && value instanceof Struct) {
                    recordKey = buildKafkaKey((Struct) value, keyFieldName);
                }

                records.add(new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        topic,
                        Schema.OPTIONAL_STRING_SCHEMA,
                        recordKey != null ? recordKey.toString() : null,
                        schema,
                        value));

                linesProcessed++;
                if (linesProcessed % 10000 == 0) {
                    log.info("Processed {} lines from {}", linesProcessed, currentFilename);
                }
            }

            if (line == null) {
                eof = true;
            }

            if (eof) {
                if (currentReader != null)
                    currentReader.close();
                if (client instanceof FtpRemoteClient ftp) {
                    ftp.completePending();
                }
                if (currentStream != null)
                    currentStream.close();

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmssSSS");
                String timestamp = LocalDateTime.now().format(formatter);
                String archiveFilename = currentFilename.replaceAll("(\\.\\w+)$", "_" + timestamp + "$1");
                String archivedPath = archiveDir + "/" + archiveFilename;

                log.info("Archiving file: {} → {}", currentStagedPath, archivedPath);
                client.moveFile(currentStagedPath, archivedPath);

                log.info("Finished processing file {} with {} lines", currentFilename, linesProcessed);
                currentReader = null;
                currentStream = null;
                currentFilename = null;
                currentStagedPath = null;
            }

        } catch (Exception e) {
            log.error("Error during polling", e);
            throw new ConnectException("Error during polling from remote server", e);
        }

        Thread.sleep(pollInterval);
        return records;
    }

    @Override
    public void stop() {
        log.info("Stopping FtpSourceTask");
        if (client != null) {
            client.disconnect();
            log.info("Disconnected from server");
        }
    }

    public void setClient(RemoteClient client) {
        this.client = client;
    }

    @Override
    public String version() {
        return VERSION;
    }

    protected String buildKafkaKey(Struct struct, String keyFieldName) {
        if (keyFieldName == null || keyFieldName.isEmpty())
            return null;

        String[] keyFields = keyFieldName.split("\\+");
        StringBuilder keyBuilder = new StringBuilder();

        for (String field : keyFields) {
            Object part = struct.get(field.trim());
            if (part != null) {
                if (keyBuilder.length() > 0)
                    keyBuilder.append("_");
                keyBuilder.append(part.toString());
            }
        }

        return keyBuilder.length() > 0 ? keyBuilder.toString() : null;
    }

    protected RecordModel buildRecordModel(String line) {
        if ("json".equals(outputFormat)) {
            String[] parts = line.split(Pattern.quote(tokenizer), -1);

            SchemaBuilder builder = SchemaBuilder.struct().optional();
            for (int i = 0; i < parts.length; i++) {
                String key = (fieldHeaders != null && i < fieldHeaders.size())
                        ? fieldHeaders.get(i)
                        : "field" + (i + 1);
                builder.field(key, Schema.OPTIONAL_STRING_SCHEMA);
            }
            Schema dynamicSchema = builder.build();

            Struct struct = new Struct(dynamicSchema);
            for (int i = 0; i < parts.length; i++) {
                String key = (fieldHeaders != null && i < fieldHeaders.size())
                        ? fieldHeaders.get(i)
                        : "field" + (i + 1);
                struct.put(key, parts[i].trim());
            }

            return new RecordModel(struct, dynamicSchema);
        } else {
            return new RecordModel(line, Schema.STRING_SCHEMA);
        }
    }
}
