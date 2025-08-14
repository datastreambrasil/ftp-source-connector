package br.com.datastreambrasil.kafka.connector.ftp;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class FtpSourceTaskTest {

    private InputStream input;

    static class TestableFtpSourceTask extends FtpSourceTask {
        public void configureForTest(String format, String tokenizer, List<String> headers) {
            this.outputFormat = format;
            this.tokenizer = tokenizer;
            this.fieldHeaders = headers;
        }

        public void injectClient(RemoteClient client) {
            this.client = client;
        }

        public RecordModel callBuildRecordModel(String line) {
            return buildRecordModel(line);
        }
    }

    private Map<String, String> createBaseConfig(String format) {
        Map<String, String> props = new HashMap<>();
        props.put(FtpSourceConnector.FTP_PROTOCOL, "sftp");
        props.put(FtpSourceConnector.FTP_HOST, "localhost");
        props.put(FtpSourceConnector.FTP_PORT, "22");
        props.put(FtpSourceConnector.FTP_USERNAME, "user");
        props.put(FtpSourceConnector.FTP_PASSWORD, "pass");
        props.put(FtpSourceConnector.FTP_DIRECTORY, "/mock");
        props.put(FtpSourceConnector.FTP_DIRECTORY_STAGE, "/mock-stage");
        props.put(FtpSourceConnector.FTP_DIRECTORY_ARCHIVE, "/mock-archive");
        props.put(FtpSourceConnector.FTP_FILE_PATTERN, ".*\\.txt");
        props.put(FtpSourceConnector.FTP_FILE_ENCODING, "UTF-8");
        props.put(FtpSourceConnector.FTP_FILE_OUTPUT_FORMAT, format);
        props.put(FtpSourceConnector.FTP_FILE_TOKENIZER, ";");
        props.put(FtpSourceConnector.FTP_FILE_HEADERS, "tipo,data,hora,code,value");
        props.put(FtpSourceConnector.FTP_KAFKA_KEY_FIELD, "tipo+code");
        props.put(FtpSourceConnector.FTP_POLL_INTERVAL, "0");
        props.put(FtpSourceConnector.FTP_MAX_RECORDS_PER_POLL, "1000");
        props.put(FtpSourceConnector.TOPIC, "test-topic");
        return props;
    }

    @BeforeEach
    void loadInputFile() {
        input = getClass().getClassLoader().getResourceAsStream("WB1.txt");
        assertNotNull(input, "Arquivo WB1.txt não encontrado em src/test/resources!");
    }

    private RemoteClient createMockClient(InputStream inputStream) throws Exception {
        RemoteClient mockClient = mock(RemoteClient.class);
        when(mockClient.listFiles(anyString(), any())).thenReturn(List.of("/stage/test.txt"), List.of());
        when(mockClient.retrieveFileStream(anyString())).thenReturn(inputStream);
        doNothing().when(mockClient).moveFile(anyString(), anyString());
        return mockClient;
    }

    @Test
    void testEachLineConvertedToJsonWithHeaders() throws Exception {
        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.injectClient(createMockClient(input));
        task.start(createBaseConfig("json"));

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertFalse(records.isEmpty());

        SourceRecord record = records.get(0);
        assertTrue(record.value() instanceof Struct);

        Struct struct = (Struct) record.value();
        assertEquals("WB", struct.getString("tipo"));
        assertEquals("20250217", struct.getString("data"));
        assertEquals("1754", struct.getString("hora"));
        assertTrue(struct.schema().fields().size() > 3);
    }

    @Test
    void testEachLineAsString() throws Exception {
        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.injectClient(createMockClient(input));
        task.start(createBaseConfig("string"));

        List<SourceRecord> records = task.poll();
        assertNotNull(records);
        assertFalse(records.isEmpty());

        SourceRecord record = records.get(0);
        assertTrue(record.value() instanceof String);

        String value = (String) record.value();
        assertTrue(value.startsWith("WB"));
        assertTrue(value.contains(";"));
    }

    @Test
    void testCompositeKafkaKeyFromMultipleFields() {
        String line = "WB;20250217;1754;284;255";
        String tokenizer = ";";
        List<String> headers = Arrays.asList("type", "date", "time", "code", "value");
        String keyFieldConfig = "type+code";

        SchemaBuilder builder = SchemaBuilder.struct().optional();
        headers.forEach(h -> builder.field(h, Schema.OPTIONAL_STRING_SCHEMA));
        Schema schema = builder.build();

        String[] parts = line.split(Pattern.quote(tokenizer), -1);
        Struct struct = new Struct(schema);
        for (int i = 0; i < headers.size(); i++) {
            struct.put(headers.get(i), parts[i]);
        }

        String[] keyFields = keyFieldConfig.split("\\+");
        StringBuilder keyBuilder = new StringBuilder();
        for (String field : keyFields) {
            Object value = struct.get(field.trim());
            if (value != null) {
                if (keyBuilder.length() > 0)
                    keyBuilder.append("_");
                keyBuilder.append(value.toString());
            }
        }
        String expectedKey = keyBuilder.toString();

        SourceRecord record = new SourceRecord(
                Collections.singletonMap("file", "example.txt"),
                Collections.singletonMap("position", 0L),
                "ftp-data-topic",
                Schema.OPTIONAL_STRING_SCHEMA,
                expectedKey,
                schema,
                struct);

        assertEquals("WB_284", record.key());
        assertEquals(expectedKey, record.key());
        assertEquals(struct, record.value());
    }

    @Test
    void testPollGeneratesCompositeKeyFromPrivateMethod() throws Exception {
        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.injectClient(createMockClient(
                new ByteArrayInputStream("WB;20250217;1754;284;255".getBytes(StandardCharsets.UTF_8))));
        task.start(createBaseConfig("json"));

        List<SourceRecord> records = task.poll();
        assertFalse(records.isEmpty());
        assertEquals("WB_284", records.get(0).key());
    }

    @Test
    void testPollingInChunks() throws Exception {
        Map<String, String> cfg = createBaseConfig("json");
        cfg.put(FtpSourceConnector.FTP_MAX_RECORDS_PER_POLL, "10");

        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.injectClient(createMockClient(input));
        task.start(cfg);

        List<SourceRecord> total = new ArrayList<>();
        List<SourceRecord> batch;
        do {
            batch = task.poll();
            total.addAll(batch);
        } while (!batch.isEmpty());

        assertEquals(32, total.size());
    }

    @Test
    void testBuildRecordModelWithJsonFormat() {
        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.configureForTest("json", ";", Arrays.asList("type", "date", "time", "code", "value"));

        String line = "WB;20250217;1754;284;255";

        RecordModel result = task.callBuildRecordModel(line);

        assertNotNull(result);
        assertTrue(result.value instanceof Struct);
        Struct struct = (Struct) result.value;

        assertEquals("WB", struct.get("type"));
        assertEquals("20250217", struct.get("date"));
        assertEquals("1754", struct.get("time"));
        assertEquals("284", struct.get("code"));
        assertEquals("255", struct.get("value"));

        assertNotNull(result.schema);
        assertEquals(Schema.Type.STRUCT, result.schema.type());
    }

    @Test
    void testBuildRecordModelWithStringFormat() {
        TestableFtpSourceTask task = new TestableFtpSourceTask();
        task.configureForTest("string", ";", null);

        String line = "RawLineContent";

        RecordModel result = task.callBuildRecordModel(line);

        assertNotNull(result);
        assertEquals("RawLineContent", result.value);
        assertEquals(Schema.STRING_SCHEMA, result.schema);
    }
}
