# Kafka EI Connector

The Kafka [connector](https://docs.wso2.com/display/EI650/Working+with+Connectors) allows you to access the [Kafka 
Producer API](http://kafka.apache.org/documentation.html#producerapi) through WSO2 EI and acts as a message producer 
that facilitates message publishing. The Kafka connector sends messages to the Kafka brokers. 

Kafka is a distributed publish-subscribe messaging system that maintains feeds of messages in topics. Producers write
data to topics and consumers read from topics. For more information on Apache Kafka, see [Apache Kafka documentation](http://kafka.apache.org/documentation.html). 



## Compatibility

| Connector version | Supported Kafka version | Supported WSO2 ESB/EI version |
| ------------- | ---------------|------------- |
| [2.0.8](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.8) | kafka_2.12-1.0.0 | EI 6.5.0   |
| [2.0.7](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.7) | kafka_2.12-1.0.0 | EI 6.5.0   |
| [2.0.6](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.6) | kafka_2.12-1.0.0 | EI 6.5.0   |
| [2.0.5](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.5) | kafka_2.12-1.0.0 |ESB 4.9.0, EI 6.2.0, EI 6.3.0, EI 6.4.0, EI 6.5.0   |
| [2.0.4](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.4) | kafka_2.12-1.0.0 |ESB 4.9.0, ESB 5.0.0, EI 6.2.0   |
| [2.0.3](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-2.0.3) | kafka_2.12-1.0.0|ESB 4.9.0, ESB 5.0.0   |
| [1.0.1](https://github.com/wso2-extensions/esb-connector-kafka/tree/org.wso2.carbon.connector.kafkaTransport-1.0.1) | kafka_2.12-1.0.0, kafka_2.12-0.11.0.0, 2.9.2-0.8.1.1 |ESB 4.9.0, ESB 5.0.0    |

## Getting started

#### Download and install the connector

1. Download the connector from the [WSO2 Store](https://store.wso2.com/store/assets/esbconnector/details/3fcaf309-1a69-4edf-870a-882bb76fdaa1) by clicking the **Download Connector** button.
2. You can then follow this [documentation](https://docs.wso2.com/display/EI650/Working+with+Connectors+via+the+Management+Console) to add the connector to your WSO2 EI instance and to enable it (via the management console).
3. For more information on using connectors and their operations in your WSO2 EI configurations, see [Using a Connector](https://docs.wso2.com/display/EI650/Using+a+Connector).
4. If you want to work with connectors via WSO2 EI Tooling, see [Working with Connectors via Tooling](https://docs.wso2.com/display/EI650/Working+with+Connectors+via+Tooling).

#### Configuring the connector operations

To get started with Kafka connector and their operations, see [Configuring Kafka Operations](docs/config.md).

## Building From the Source

Follow the steps given below to build the Kafka connector from the source code:

1. Get a clone or download the source from [Github](https://github.com/wso2-extensions/esb-connector-kafka).
2. Run the following Maven command from the `esb-connector-kafka` directory: `mvn clean install`.
3. The Kafka connector zip file is created in the `esb-connector-kafka/target` directory

## How You Can Contribute

As an open source project, WSO2 extensions welcome contributions from the community.
Check the [issue tracker](https://github.com/wso2-extensions/esb-connector-kafka/issues) for open issues that interest you. We look forward to receiving your contributions.


## How to test Avro mode
### Configure Schema Registry
1. Setup confluent platform with schema registry locally https://docs.confluent.io/current/schema-registry/installation/index.html
2. Navigate to the Control Center web interface at http://localhost:9021/.
3. Create topic test-topic in using Center web interface
4. Add sample schema to test-topic https://gist.github.com/jenananthan/994df1cc09d12b78ea2ebca50a7503f4#file-userschema-json
5. Download the schema and note down the schema id

### Configure WSO2 EI 6.2.0
1.Build and upload kafka connector
2.Enable the kafka connector via carbon console
3.Store the schema in registry location
e.g location conf:/avroschema/userschema.json
. Schema : https://gist.github.com/jenananthan/994df1cc09d12b78ea2ebca50a7503f4#file-userschema-json

### Configure Proxy
Set below properties to use by kafka connector. Set the schemaID and schema registry location
```
<property name="ENABLE_AVRO" type="BOOLEAN" value="true"/>
<property name="AVRO_SCHEMA_ID" type="INTEGER" value="2"/>
<property name="AVRO_SCHEMA_LOCATION" value="conf:/avroschema/userschema.json"/>
<property name="messageType" scope="axis2" value="application/json"/>
```
Sample Proxy: https://gist.github.com/jenananthan/994df1cc09d12b78ea2ebca50a7503f4#file-customconnector-proxy-xml
### Produce message
```
curl --location --request POST 'http://localhost:8280/services/CustomConnector' \
--header 'Content-Type: application/json' \
--data-raw '{"f1": "value"}'
```

### Consume message using confluent avro client
```
bin/kafka-avro-console-consumer --topic test-topic  --bootstrap-server localhost:9092
```

Avro client usage : https://docs.confluent.io/1.0.1/quickstart.html
