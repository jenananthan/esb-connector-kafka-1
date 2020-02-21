/*
 *  Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.connector;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.axiom.om.OMOutputFormat;
import org.apache.axis2.AxisFault;
import org.apache.axis2.transport.MessageFormatter;
import org.apache.axis2.transport.base.BaseUtils;
import org.apache.axis2.util.MessageProcessorSelector;
import org.apache.commons.io.output.WriterOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseLog;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.Value;
import org.wso2.carbon.connector.core.AbstractConnector;
import org.wso2.carbon.connector.core.ConnectException;
import org.wso2.carbon.connector.core.util.ConnectorUtils;
import org.wso2.carbon.mediation.registry.WSO2Registry;
import org.wso2.carbon.registry.api.RegistryException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Produce the messages to the kafka brokers.
 */
public class KafkaProduceConnector extends AbstractConnector {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    public void connect(MessageContext messageContext) throws ConnectException {

        SynapseLog log = getLog(messageContext);
        log.auditLog("SEND : send message to  Broker lists");
        try {
            // Get the maximum pool size
            String maxPoolSize = (String) messageContext
                    .getProperty(KafkaConnectConstants.CONNECTION_POOL_MAX_SIZE);
            // Read the topic from the parameter
            String topic = lookupTemplateParameter(messageContext, KafkaConnectConstants.PARAM_TOPIC);
            //Generate the key.
            String key = lookupTemplateParameter(messageContext, KafkaConnectConstants.KEY_MESSAGE);
            if(StringUtils.isEmpty(key)) {
                key = String.valueOf(UUID.randomUUID());
            }
            //Read the partition No from the parameter
            String partitionNo = lookupTemplateParameter(messageContext, KafkaConnectConstants.PARTITION_NO);
            String message = getMessage(messageContext);
            org.apache.kafka.common.header.Headers headers = getDynamicParameters(messageContext, topic);
            if (StringUtils.isEmpty(maxPoolSize) || KafkaConnectConstants.DEFAULT_CONNECTION_POOL_MAX_SIZE
                    .equals(maxPoolSize)) {
                //Make the producer connection without connection pool
                sendWithoutPool(messageContext, topic, partitionNo, key, message, headers);
            } else {
                //Make the producer connection with connection pool
                sendWithPool(messageContext, topic, partitionNo, key, message, headers);
            }
        } catch (AxisFault axisFault) {
            handleException("Kafka producer connector " +
                    ": Error sending the message to broker lists", axisFault, messageContext);
        }
    }

    /**
     * Get the messages from the message context and format the messages.
     */
    private String getMessage(MessageContext messageContext) throws AxisFault {
        Axis2MessageContext axisMsgContext = (Axis2MessageContext) messageContext;
        org.apache.axis2.context.MessageContext msgContext = axisMsgContext.getAxis2MessageContext();
        return formatMessage(msgContext);
    }

    /**
     * Will generate the dynamic parameters from message context parameter
     *
     * @param messageContext The message contest
     * @param topicName      The topicName to generate the dynamic parameters
     * @return extract the value's from properties and make its as header
     */
    private org.apache.kafka.common.header.Headers getDynamicParameters(MessageContext messageContext,
                                                                        String topicName) {
        org.apache.kafka.common.header.Headers headers = new RecordHeaders();
        String key = KafkaConnectConstants.METHOD_NAME + topicName;
        Map<String, Object> propertiesMap = (((Axis2MessageContext) messageContext).getProperties());
        for (String keyValue : propertiesMap.keySet()) {
            if (keyValue.startsWith(key)) {
                Value propertyValue = (Value) propertiesMap.get(keyValue);
                headers.add(keyValue.substring(key.length() + 1, keyValue.length()), propertyValue.
                        evaluateValue(messageContext).getBytes());
            }
        }
        return headers;
    }

    /**
     * Format the messages when the messages are sent to the kafka broker
     */
    private static String formatMessage(org.apache.axis2.context.MessageContext messageContext)
            throws AxisFault {
        OMOutputFormat format = BaseUtils.getOMOutputFormat(messageContext);
        MessageFormatter messageFormatter = MessageProcessorSelector.getMessageFormatter(messageContext);
        StringWriter stringWriter = new StringWriter();
        OutputStream out = new WriterOutputStream(stringWriter, format.getCharSetEncoding());
        try {
            messageFormatter.writeTo(messageContext, format, out, true);
        } catch (IOException e) {
            throw new AxisFault("The Error occurs while formatting the message", e);
        } finally {
            try {
                out.close();
            } catch (Exception e) {
                throw new AxisFault("The Error occurs while closing the output stream", e);
            }
        }
        return stringWriter.toString();
    }

    /**
     * Send the messages to the kafka broker with topic and the key that is optional.
     *
     * @param producer    The instance of the Kafka producer.
     * @param topic       The topic to send the message.
     * @param partitionNo The partition Number of the broker.
     * @param key         The key.
     * @param message     The message that send to kafka broker.
     * @param headers     The kafka headers
     */
    private void send(KafkaProducer producer, String topic, String partitionNo, String key,
                      String message, org.apache.kafka.common.header.Headers headers, MessageContext messageContext)
            throws ExecutionException, InterruptedException {

        Integer partitionNumber = null;
        try {
            if (!StringUtils.isEmpty(partitionNo)) {
                partitionNumber = Integer.parseInt(partitionNo);
            }
        } catch (NumberFormatException e) {
            log.error("Invalid Partition Number, hence passing null as the partition number", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("Sending message with the following properties : Topic=" + topic + ", Partition Number=" +
                    partitionNumber + ", Key=" + key + ", Message=" + message + ", Headers=" + headers);
        }
        Future<RecordMetadata> metaData = null;
        boolean isAvroKey = Boolean.parseBoolean((String)messageContext.getProperty(KafkaConnectConstants.ENABLE_AVRO_KEY));
        boolean isAvroValue = Boolean.parseBoolean((String)messageContext.getProperty(KafkaConnectConstants.ENABLE_AVRO_VALUE));

        if(isAvroKey && isAvroValue) {
            log.debug("Avro mode enabled for key and value");
            try {
                log.debug("Serialize Key");
                Bytes keyAvro = getAvroMessage(key, messageContext, true);
                Bytes messageAvro  = getAvroMessage(message, messageContext, false);
                metaData = producer.send(new ProducerRecord<>(topic, partitionNumber, keyAvro, messageAvro, headers));
            } catch (RegistryException | IOException e) {
                handleException("Error while getting avro serialized  message : " + key, e, messageContext);
            }
        } else if (!isAvroKey && isAvroValue) {
            log.debug("Avro mode enabled for value");
            try {
                Bytes messageAvro  = getAvroMessage(message, messageContext, false);
                metaData = producer.send(new ProducerRecord<>(topic, partitionNumber, key, messageAvro, headers));
            } catch (RegistryException | IOException e) {
                handleException("Error while getting avro serialized  message : " + key, e, messageContext);
            }
        } else {
            metaData = producer.send(new ProducerRecord<>(topic, partitionNumber, key, message, headers));
        }




        messageContext.setProperty("topic", metaData.get().topic());
        messageContext.setProperty("offset", metaData.get().offset());
        messageContext.setProperty("partition", metaData.get().partition());

        if (log.isDebugEnabled()) {
            log.debug("Flushing producer after sending the message");
        }
        producer.flush();
    }

    /**
     * Send the messages with connection pool.
     *
     * @param messageContext The message context.
     * @param topic          The topic.
     * @param partitionNo    The partition Number of the broker.
     * @param key            The key.
     * @param message        The message.
     * @param headers        The custom header.
     * @throws ConnectException The Exception while create the connection from the Connection pool.
     */
    private void sendWithPool(MessageContext messageContext, String topic, String partitionNo, String key,
                              String message, org.apache.kafka.common.header.Headers headers)
            throws ConnectException {

        KafkaProducer producer = KafkaConnectionPool.getConnectionFromPool();
        if (producer == null) {
            KafkaConnectionPool.initialize(messageContext);
        }

        try {
            if (producer != null) {
                if (log.isDebugEnabled()) {
                    log.debug("Sending message to broker list WITH connection pool");
                }
                send(producer, topic, partitionNo, key, message, headers, messageContext);
            } else {
                //If any error occurs while getting the connection from the pool.
                sendWithoutPool(messageContext, topic, partitionNo, key, message, headers);
            }
        } catch (Exception e) {
            handleException("Kafka producer connector:" +
                    "Error sending the message to broker lists with connection Pool", e, messageContext);
        } finally {
            //Close the producer pool connections to all kafka brokers.
            // Also closes the zookeeper client connection if any
            if (producer != null) {
                KafkaConnectionPool.returnConnectionToPool(producer);
            }
        }
    }

    /**
     * Send the messages without connection pool.
     *
     * @param messageContext The message context.
     * @param topic          The topic.
     * @param partitionNo    The partition number of the broker.
     * @param key            The key.
     * @param message        The message.
     * @param headers        The Kafka headers
     * @throws ConnectException The Exception while create the Kafka Connection.
     */
    private void sendWithoutPool(MessageContext messageContext, String topic, String partitionNo, String key,
                                 String message, org.apache.kafka.common.header.Headers headers)
            throws ConnectException {
        KafkaConnection kafkaConnection = new KafkaConnection();
        KafkaProducer producer = kafkaConnection.createNewConnection(messageContext);

        if (log.isDebugEnabled()) {
            log.debug("Sending message to broker list WITHOUT connection pool");
        }

        try {
            send(producer, topic, partitionNo, key, message, headers, messageContext);
        } catch (Exception e) {
            handleException("Kafka producer connector:" +
                    "Error sending the message to broker lists without connection Pool", e, messageContext);
        } finally {
            //Close the producer pool connections to all kafka brokers.
            // Also closes the zookeeper client connection if any
            if (producer != null) {
                producer.close();
            }
        }
    }

    /**
     * Read the value from the input parameter
     */
    private static String lookupTemplateParameter(MessageContext messageContext, String paramName) {
        return (String) ConnectorUtils.lookupTemplateParamater(messageContext, paramName);
    }

    private Bytes getAvroMessage(String jsonMessage, MessageContext messageContext, boolean isKey) throws RegistryException, IOException {
        log.debug("Get avro message");
        //Get avrom schema from wso2 registry
        String avroSchema =  getSchema(messageContext, isKey);
        Schema schema = new Schema.Parser().parse(avroSchema);
        //Convert json message to generic record for serialization
        JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, jsonMessage);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        GenericRecord avroRecord = datumReader.read(null, jsonDecoder);
        //Prepend schema ID
        int schemaID = getSchemaID(messageContext, isKey);
        log.debug(String.format("Start avro serialization. Schema: %s . Schema ID: %s. Message : %s.",avroSchema, schemaID, messageContext));
        final byte MAGIC_BYTE = 0x0;
        final int idSize = 4;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(MAGIC_BYTE);
        out.write(ByteBuffer.allocate(idSize).putInt(schemaID).array());

        //Serialize the generic record
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<Object>  writer = new GenericDatumWriter<>(schema);
        writer.write(avroRecord, encoder);
        encoder.flush();

        byte[] bytes = out.toByteArray();
        out.close();
        return new Bytes(bytes);
    }

    private int getSchemaID(MessageContext messageContext, boolean isKey)  {
        String schemaID;
        if(isKey) {
            schemaID = lookupTemplateParameter(messageContext, KafkaConnectConstants.KEY_SCHEMA_ID);
        } else {
            schemaID = lookupTemplateParameter(messageContext, KafkaConnectConstants.VALUE_SCHEMA_ID);
        }
        return Integer.parseInt(schemaID);
    }

    private  String getSchema(MessageContext messageContext, boolean isKey) throws RegistryException {
        String schemaLocation;
        if(isKey) {
            schemaLocation = lookupTemplateParameter(messageContext, KafkaConnectConstants.KEY_SCHEMA);
        } else {
            schemaLocation = lookupTemplateParameter(messageContext, KafkaConnectConstants.VALUE_SCHEMA);
        }
        return new String((byte[]) ((WSO2Registry)messageContext.getConfiguration().getRegistry())
                .getResource(schemaLocation).getContent());
    }
}