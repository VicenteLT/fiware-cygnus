/**
 * Copyright 2014-2020 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FIWARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */

package com.telefonica.iot.cygnus.sinks;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericAggregator;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericColumnAggregator;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericRowAggregator;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusBadContextData;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import com.telefonica.iot.cygnus.utils.CommonConstants;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import org.apache.flume.Context;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;


import static com.telefonica.iot.cygnus.utils.CommonUtilsForTests.getTestTraceHead;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.*;

public class NGSIMongoSinkTest {

    public static final String OPEN_ENTITY_CHAR = "(";
    public static final String CLOSE_ENTITY_CHAR = ")";
    public static final String SEPARATOR_CHAR = ",";
    public static final String QUOTATION_MARK_CHAR = "";

    public NGSIMongoSinkTest() {
        LogManager.getRootLogger().setLevel(Level.FATAL);
    }


    private NotifyContextRequest.ContextElement createContextElement() {
        NotifyContextRequest notifyContextRequest = new NotifyContextRequest();
        NotifyContextRequest.ContextMetadata contextMetadata = new NotifyContextRequest.ContextMetadata();
        contextMetadata.setName("location");
        contextMetadata.setType("string");
        contextMetadata.setContextMetadata(new JsonPrimitive("WGS84"));
        ArrayList<NotifyContextRequest.ContextMetadata> metadata = new ArrayList<>();
        metadata.add(contextMetadata);
        NotifyContextRequest.ContextAttribute contextAttribute1 = new NotifyContextRequest.ContextAttribute();
        contextAttribute1.setName("someName1");
        contextAttribute1.setType("someType1");
        contextAttribute1.setContextValue(new JsonPrimitive("-3.7167, 40.3833"));
        contextAttribute1.setContextMetadata(metadata);
        NotifyContextRequest.ContextAttribute contextAttribute2 = new NotifyContextRequest.ContextAttribute();
        contextAttribute2.setName("someName2");
        contextAttribute2.setType("someType2");
        contextAttribute2.setContextValue(new JsonPrimitive("someValue2"));
        contextAttribute2.setContextMetadata(null);
        ArrayList<NotifyContextRequest.ContextAttribute> attributes = new ArrayList<>();
        attributes.add(contextAttribute1);
        attributes.add(contextAttribute2);
        NotifyContextRequest.ContextElement contextElement = new NotifyContextRequest.ContextElement();
        contextElement.setId("someId");
        contextElement.setType("someType");
        contextElement.setIsPattern("false");
        contextElement.setAttributes(attributes);
        return contextElement;
    } // createContextElement

    private Context createContextforNativeTypes(String attrPersistence, String batchSize, String batchTime, String batchTTL,
                                                String dataModel, String enableEncoding, String enableGrouping, String enableLowercase, String host,
                                                String password, String port, String username, String cache, String attrNativeTypes) {
        Context context = new Context();
        context.put("attr_persistence", attrPersistence);
        context.put("batch_size", batchSize);
        context.put("batch_time", batchTime);
        context.put("batch_ttl", batchTTL);
        context.put("data_model", dataModel);
        context.put("enable_encoding", enableEncoding);
        context.put("enable_grouping", enableGrouping);
        context.put("enable_lowercase", enableLowercase);
        context.put("mysql_host", host);
        context.put("mysql_password", password);
        context.put("mysql_port", port);
        context.put("mysql_username", username);
        context.put("backend.enable_cache", cache);
        context.put("attr_native_types", attrNativeTypes);
        return context;
    } // createContext

    private NotifyContextRequest.ContextElement createContextElementForNativeTypes() {
        NotifyContextRequest notifyContextRequest = new NotifyContextRequest();
        NotifyContextRequest.ContextMetadata contextMetadata = new NotifyContextRequest.ContextMetadata();
        contextMetadata.setName("someString");
        contextMetadata.setType("string");
        ArrayList<NotifyContextRequest.ContextMetadata> metadata = new ArrayList<>();
        metadata.add(contextMetadata);
        NotifyContextRequest.ContextAttribute contextAttribute1 = new NotifyContextRequest.ContextAttribute();
        contextAttribute1.setName("someNumber");
        contextAttribute1.setType("number");
        contextAttribute1.setContextValue(new JsonPrimitive(2));
        contextAttribute1.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute2 = new NotifyContextRequest.ContextAttribute();
        contextAttribute2.setName("somneBoolean");
        contextAttribute2.setType("Boolean");
        contextAttribute2.setContextValue(new JsonPrimitive(true));
        contextAttribute2.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute3 = new NotifyContextRequest.ContextAttribute();
        contextAttribute3.setName("someDate");
        contextAttribute3.setType("DateTime");
        contextAttribute3.setContextValue(new JsonPrimitive("2016-09-21T01:23:00.00Z"));
        contextAttribute3.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute4 = new NotifyContextRequest.ContextAttribute();
        contextAttribute4.setName("someGeoJson");
        contextAttribute4.setType("geo:json");
        contextAttribute4.setContextValue(new JsonPrimitive("{\"type\": \"Point\",\"coordinates\": [-0.036177,39.986159]}"));
        contextAttribute4.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute5 = new NotifyContextRequest.ContextAttribute();
        contextAttribute5.setName("someJson");
        contextAttribute5.setType("json");
        contextAttribute5.setContextValue(new JsonPrimitive("{\"String\": \"string\"}"));
        contextAttribute5.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute6 = new NotifyContextRequest.ContextAttribute();
        contextAttribute6.setName("someString");
        contextAttribute6.setType("string");
        contextAttribute6.setContextValue(new JsonPrimitive("foo"));
        contextAttribute6.setContextMetadata(null);
        NotifyContextRequest.ContextAttribute contextAttribute7 = new NotifyContextRequest.ContextAttribute();
        contextAttribute7.setName("someString2");
        contextAttribute7.setType("string");
        contextAttribute7.setContextValue(new JsonPrimitive(""));
        contextAttribute7.setContextMetadata(null);
        ArrayList<NotifyContextRequest.ContextAttribute> attributes = new ArrayList<>();
        attributes.add(contextAttribute1);
        attributes.add(contextAttribute2);
        attributes.add(contextAttribute3);
        attributes.add(contextAttribute4);
        attributes.add(contextAttribute5);
        attributes.add(contextAttribute6);
        attributes.add(contextAttribute7);
        NotifyContextRequest.ContextElement contextElement = new NotifyContextRequest.ContextElement();
        contextElement.setId("someId");
        contextElement.setType("someType");
        contextElement.setIsPattern("false");
        contextElement.setAttributes(attributes);
        return contextElement;
    } // createContextElementForNativeTypes

    public NGSIBatch setUpBatch() {
        String timestamp = "1461136795801";
        String correlatorId = "123456789";
        String transactionId = "123456789";
        String originalService = "someService";
        String originalServicePath = "somePath";
        String mappedService = "newService";
        String mappedServicePath = "newPath";
        String destination = "someDestination";
        Map<String, String> headers = new HashMap<>();
        headers.put(NGSIConstants.FLUME_HEADER_TIMESTAMP, timestamp);
        headers.put(CommonConstants.HEADER_CORRELATOR_ID, correlatorId);
        headers.put(NGSIConstants.FLUME_HEADER_TRANSACTION_ID, transactionId);
        headers.put(CommonConstants.HEADER_FIWARE_SERVICE, originalService);
        headers.put(CommonConstants.HEADER_FIWARE_SERVICE_PATH, originalServicePath);
        headers.put(NGSIConstants.FLUME_HEADER_MAPPED_SERVICE, mappedService);
        headers.put(NGSIConstants.FLUME_HEADER_MAPPED_SERVICE_PATH, mappedServicePath);
        NotifyContextRequest.ContextElement contextElement = createContextElementForNativeTypes();
        NotifyContextRequest.ContextElement contextElement2 = createContextElement();
        NGSIEvent ngsiEvent = new NGSIEvent(headers, contextElement.toString().getBytes(), contextElement, null);
        NGSIEvent ngsiEvent2 = new NGSIEvent(headers, contextElement2.toString().getBytes(), contextElement2, null);
        NGSIBatch batch = new NGSIBatch();
        batch.addEvent(destination, ngsiEvent);
        batch.addEvent(destination, ngsiEvent2);
        return batch;
    }

    @Test
    public void testNativeTypeColumnBatch() throws CygnusBadConfiguration, CygnusRuntimeError, CygnusPersistenceError, CygnusBadContextData {
        NGSIBatch batch = setUpBatch();
        String destination = "someDestination";NGSIMongoSink ngsiMongoSink = new NGSIMongoSink();
        ngsiMongoSink.configure(createContextforNativeTypes("column", null, null, null, null, null, null, null, null, null, null, null, null, null));
        try {
            batch.startIterator();
            NGSIGenericAggregator aggregator = new NGSIGenericColumnAggregator();
            while (batch.hasNext()) {
                destination = batch.getNextDestination();
                ArrayList<NGSIEvent> events = batch.getNextEvents();
                aggregator.setService(events.get(0).getServiceForNaming(false));
                aggregator.setServicePathForData(events.get(0).getServicePathForData());
                aggregator.setServicePathForNaming(events.get(0).getServicePathForNaming(false, false));
                aggregator.setEntityForNaming(events.get(0).getEntityForNaming(false, false, false));
                aggregator.setEntityType(events.get(0).getEntityTypeForNaming(false, false));
                aggregator.setAttribute(events.get(0).getAttributeForNaming(false));
                aggregator.setDbName(ngsiMongoSink.buildDbName(aggregator.getService()));
                aggregator.setEnableRecvTimeDateFormat(true);
                aggregator.setAttrMetadataStore(true);
                aggregator.setCollectionName(ngsiMongoSink.buildCollectionName(aggregator.getServicePathForNaming(), aggregator.getEntityForNaming(), aggregator.getAttribute()));
                aggregator.initialize(events.get(0));
                for (NGSIEvent event : events) {
                    aggregator.aggregate(event);
                }
            }
            ArrayList<String> keysToCrop = ngsiMongoSink.getKeysToCrop(false);
            LinkedHashMap<String, ArrayList<JsonElement>> cropedAggregation = NGSIUtils.cropLinkedHashMap(aggregator.getAggregationToPersist(), keysToCrop);
            ArrayList<JsonObject> jsonObjects = NGSIUtils.linkedHashMapToJsonList(cropedAggregation);
            ArrayList<Document> documents = new ArrayList<>();
            for (JsonObject jsonObject : jsonObjects) {
                documents.add(Document.parse(jsonObject.toString()));
            }
            System.out.println("[NGSIMongoSinkTest.testNativeTypeColumnBatch: " + documents);
            String correctBatch = "[Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", someNumber=2, someNumber_md=[], somneBoolean=true, somneBoolean_md=[], someDate=2016-09-21T01:23:00.00Z, someDate_md=[], someGeoJson={\"type\": \"Point\",\"coordinates\": [-0.036177,39.986159]}, someGeoJson_md=[], someJson={\"String\": \"string\"}, someJson_md=[], someString=foo, someString_md=[], someString2=, someString2_md=[]}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", someName1=-3.7167, 40.3833, someName1_md=[{\"name\":\"location\",\"type\":\"string\",\"value\":\"WGS84\"}], someName2=someValue2, someName2_md=[]}}]";
            if (documents.toString().equals(correctBatch)) {
                assertTrue(true);
            } else {
                assertFalse(true);
            }
        } catch (Exception e) {
            System.out.println(e);
            assertFalse(true);
        }
    }

    @Test
    public void testNativeTypeRowBatch() throws CygnusBadConfiguration, CygnusRuntimeError, CygnusPersistenceError, CygnusBadContextData {
        NGSIBatch batch = setUpBatch();
        String destination = "someDestination";NGSIMongoSink ngsiMongoSink = new NGSIMongoSink();
        ngsiMongoSink.configure(createContextforNativeTypes("row", null, null, null, null, null, null, null, null, null, null, null, null, null));
        try {
            batch.startIterator();
            while (batch.hasNext()) {
                destination = batch.getNextDestination();
                ArrayList<NGSIEvent> events = batch.getNextEvents();
                NGSIGenericAggregator aggregator = new NGSIGenericRowAggregator();
                aggregator.setService(events.get(0).getServiceForNaming(false));
                aggregator.setServicePathForData(events.get(0).getServicePathForData());
                aggregator.setServicePathForNaming(events.get(0).getServicePathForNaming(false, false));
                aggregator.setEntityForNaming(events.get(0).getEntityForNaming(false, false, false));
                aggregator.setEntityType(events.get(0).getEntityTypeForNaming(false, false));
                aggregator.setAttribute(events.get(0).getAttributeForNaming(false));
                aggregator.setDbName(ngsiMongoSink.buildDbName(aggregator.getService()));
                aggregator.setEnableRecvTimeDateFormat(true);
                aggregator.setCollectionName(ngsiMongoSink.buildCollectionName(aggregator.getServicePathForNaming(), aggregator.getEntityForNaming(), aggregator.getAttribute()));
                aggregator.initialize(events.get(0));
                for (NGSIEvent event : events) {
                    aggregator.aggregate(event);
                }
                ArrayList<String> keysToCrop = ngsiMongoSink.getKeysToCrop(true);
                LinkedHashMap<String, ArrayList<JsonElement>> cropedAggregation = NGSIUtils.cropLinkedHashMap(aggregator.getAggregationToPersist(), keysToCrop);
                ArrayList<JsonObject> jsonObjects = NGSIUtils.linkedHashMapToJsonList(cropedAggregation);
                ArrayList<Document> documents = new ArrayList<>();
                for (JsonObject jsonObject : jsonObjects) {
                    documents.add(Document.parse(jsonObject.toString()));
                }
                System.out.println("[NGSIMongoSinkTest.testNativeTypeRowBatch: " + documents);
                String correctBatch = "[Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someNumber, attrType=number, attrValue=2}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=somneBoolean, attrType=Boolean, attrValue=true}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someDate, attrType=DateTime, attrValue=2016-09-21T01:23:00.00Z}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someGeoJson, attrType=geo:json, attrValue={\"type\": \"Point\",\"coordinates\": [-0.036177,39.986159]}}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someJson, attrType=json, attrValue={\"String\": \"string\"}}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someString, attrType=string, attrValue=foo}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someString2, attrType=string, attrValue=}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someName1, attrType=someType1, attrValue=-3.7167, 40.3833}}, Document{{recvTime=" + new Date(Long.parseLong("1461136795801")) + ", attrName=someName2, attrType=someType2, attrValue=someValue2}}]";
                if (documents.toString().equals(correctBatch)) {
                    assertTrue(true);
                } else {
                    assertFalse(true);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
            assertFalse(true);
        }
    }

}
