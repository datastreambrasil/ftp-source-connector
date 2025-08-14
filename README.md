# FTP/SFTP Source Connector for Kafka Connect

This Kafka Connect Source Connector allows ingestion of flat files (e.g., `.txt`) from remote FTP or SFTP servers into Apache Kafka.  
Each file is read line by line, and each line is sent as an independent Kafka record.

---

## Features

- Supports FTP and SFTP protocols  
- Processes files line-by-line
- Configurable:
  - File encoding (`ftp.file.encoding`)
  - Field delimiter (`ftp.file.tokenizer`)
  - File matching pattern (`ftp.file.pattern`)
- Converts each line into:
  - A plain `String`, or
  - A structured `JSON` (`Schema + Struct`)
- Optional field mapping via `ftp.file.headers`
- Optional Kafka key based on one or more fields (`ftp.kafka.key.field`)
- Limit records returned per poll (`ftp.max.records.per.poll`)
- Moves files to:
  - A staging folder before processing
  - An archive folder after processing
- Archives include a timestamp in filename  
  Format: `yyyyMMdd_HHmmssSSS` (e.g., `WB1_20250408_121045123.txt`)

---

## Configuration Example

```json
{
  "name": "ftp-source",
  "config": {
    "connector.class": "br.com.datastreambrasil.kafka.connector.ftp.FtpSourceConnector",
    "ftp.protocol": "sftp",
    "ftp.host": "ftp.example.com",
    "ftp.port": "22",
    "ftp.username": "username",
    "ftp.password": "password",
    "ftp.directory": "/input",
    "ftp.directory.stage": "/staging",
    "ftp.directory.archive": "/archive",
    "ftp.file.pattern": ".*\\.txt",
    "ftp.file.encoding": "UTF-8",
    "ftp.file.output.format": "json",
    "ftp.file.tokenizer": ";",
    "ftp.file.headers": "type,date,time,code,value",
    "ftp.kafka.key.field": "type+code",
    "ftp.poll.interval.ms": "10000",
    "ftp.max.records.per.poll": "1000",
    "topic": "ftp-data-topic",
    "tasks.max": "1"
  }
}
```

---

## JSON Output Example

If `ftp.file.output.format=json` and `ftp.file.headers` are provided, each line will be transformed into a Kafka Connect `Struct` with dynamic schema.

Sample file content:
```
WB;20250217;1754;284;255
```

Using headers: `type,date,time,code,value`  
Key config: `ftp.kafka.key.field=type+code`

Kafka message:
```json
{
  "key": "WB_284",
  "schema": {
    "type": "struct",
    "fields": [
      { "field": "type", "type": "string" },
      { "field": "date", "type": "string" },
      { "field": "time", "type": "string" },
      { "field": "code", "type": "string" },
      { "field": "value", "type": "string" }
    ],
    "optional": true
  },
  "payload": {
    "type": "WB",
    "date": "20250217",
    "time": "1754",
    "code": "284",
    "value": "255"
  }
}
```

If the number of headers is less than the number of columns in the file, remaining fields will be named automatically as `"field6"`, `"field7"`, etc.

---

## Kafka Key Composition

You can define a message key using one or more fields from the parsed line.  
Use the `ftp.kafka.key.field` parameter to configure it:

- Single field key:
  ```json
  "ftp.kafka.key.field": "code"
  ```

- Composite key using multiple fields:
  ```json
  "ftp.kafka.key.field": "type+code"
  ```

The final key will be a string with field values joined by underscore `_`.  
For example: `WB_284`

If any of the referenced fields do not exist in the parsed data, they will be ignored in the key composition.

---

## Project Structure

- `FtpSourceConnector` – Kafka Connector class
- `FtpSourceTask` – Core file polling and processing logic
- `FtpRemoteClient` / `SftpRemoteClient` – FTP/SFTP integration
- `RemoteClient` – Interface abstraction

---

## License

This project is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

It uses libraries from the Apache Software Foundation (Apache Kafka, Apache Commons Net, Apache Mina SSHD) and other open-source projects.
