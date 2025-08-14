package br.com.datastreambrasil.kafka.connector.ftp;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FtpSourceConnector extends SourceConnector {

        public static final String VERSION = "1.0.0";

        public static final String FTP_PROTOCOL = "ftp.protocol";
        public static final String FTP_HOST = "ftp.host";
        public static final String FTP_PORT = "ftp.port";
        public static final String FTP_USERNAME = "ftp.username";
        public static final String FTP_PASSWORD = "ftp.password";

        public static final String FTP_DIRECTORY = "ftp.directory";
        public static final String FTP_DIRECTORY_STAGE = "ftp.directory.stage";
        public static final String FTP_DIRECTORY_ARCHIVE = "ftp.directory.archive";

        public static final String FTP_FILE_PATTERN = "ftp.file.pattern";
        public static final String FTP_FILE_ENCODING = "ftp.file.encoding";
        public static final String FTP_FILE_OUTPUT_FORMAT = "ftp.file.output.format";
        public static final String FTP_FILE_TOKENIZER = "ftp.file.tokenizer";
        public static final String FTP_FILE_HEADERS = "ftp.file.headers";
        public static final String FTP_KAFKA_KEY_FIELD = "ftp.kafka.key.field";

        public static final String FTP_MAX_RECORDS_PER_POLL = "ftp.max.records.per.poll";

        public static final String FTP_POLL_INTERVAL = "ftp.poll.interval.ms";

        public static final String TOPIC = "topic";

        private Map<String, String> config;

        @Override
        public void start(Map<String, String> props) {
                this.config = props;
        }

        @Override
        public Class<? extends Task> taskClass() {
                return FtpSourceTask.class;
        }

        @Override
        public List<Map<String, String>> taskConfigs(int maxTasks) {
                List<Map<String, String>> configs = new ArrayList<>();
                for (int i = 0; i < maxTasks; i++) {
                        configs.add(config);
                }
                return configs;
        }

        @Override
        public void stop() {
                // Do Nothing
        }

        @Override
        public ConfigDef config() {
                return new ConfigDef()
                                .define(FTP_PROTOCOL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                                                "Protocol to use: ftp or sftp")
                                .define(FTP_HOST, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                                                "Hostname or IP address of the FTP/SFTP server")
                                .define(FTP_PORT, ConfigDef.Type.INT, ConfigDef.Importance.MEDIUM,
                                                "Port number of the FTP/SFTP server")
                                .define(FTP_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                                                "Username for authentication")
                                .define(FTP_PASSWORD, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH,
                                                "Password for authentication")
                                .define(FTP_DIRECTORY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                                                "Directory where input files are located")
                                .define(FTP_DIRECTORY_STAGE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM,
                                                "Directory for staging files during processing")
                                .define(FTP_DIRECTORY_ARCHIVE, ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM,
                                                "Directory for archiving files after processing")
                                .define(FTP_FILE_PATTERN, ConfigDef.Type.STRING, ".*\\.txt",
                                                ConfigDef.Importance.MEDIUM,
                                                "Regex pattern to match filenames for processing")
                                .define(FTP_FILE_ENCODING, ConfigDef.Type.STRING, "UTF-8", ConfigDef.Importance.LOW,
                                                "Character encoding used to read the files")
                                .define(FTP_FILE_OUTPUT_FORMAT, ConfigDef.Type.STRING, "string",
                                                ConfigDef.Importance.LOW,
                                                "Format for output records: 'string' or 'json'")
                                .define(FTP_FILE_TOKENIZER, ConfigDef.Type.STRING, ";",
                                                ConfigDef.Importance.LOW,
                                                "Delimiter used to split each line when format is 'json'")
                                .define(FTP_FILE_HEADERS, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                                                "Optional comma-separated list of field names to be used as JSON keys")
                                .define(FTP_KAFKA_KEY_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.LOW,
                                                "Field name from headers to be used as Kafka message key")
                                .define(FTP_POLL_INTERVAL, ConfigDef.Type.INT, 10000, ConfigDef.Importance.LOW,
                                                "Polling interval in milliseconds")
                                .define(FTP_MAX_RECORDS_PER_POLL, ConfigDef.Type.INT, 1000, ConfigDef.Importance.LOW,
                                                "Maximum number of records returned per poll")
                                .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                                                "Kafka topic to publish the file data");
        }

        @Override
        public String version() {
                return VERSION;
        }
}
