/**
 * Copyright 2015-2017 Telefonica Investigación y Desarrollo, S.A.U
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
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericAggregator;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericColumnAggregator;
import com.telefonica.iot.cygnus.aggregation.NGSIGenericRowAggregator;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElement;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusCappingError;
import com.telefonica.iot.cygnus.errors.CygnusExpiratingError;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import static com.telefonica.iot.cygnus.sinks.NGSIMongoBaseSink.LOGGER;
import com.telefonica.iot.cygnus.utils.CommonUtils;

import java.util.*;

import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import org.bson.Document;
import org.apache.flume.Context;

/**
 * @author frb
 * @author xdelox
 * 
 * Detailed documentation can be found at:
 * https://github.com/telefonicaid/fiware-cygnus/blob/master/doc/flume_extensions_catalogue/ngsi_mongo_sink.md
 */
public class NGSIMongoSink extends NGSIMongoBaseSink {


    private static final String DEFAULT_ATTR_NATIVE_TYPES = "false";
    private static final String OPEN_ENTITY_CHAR = "(";
    private static final String CLOSE_ENTITY_CHAR = ")";
    private static final String SEPARATOR_CHAR = ",";
    private static final String QUOTATION_MARK_CHAR = "";
    
    private long collectionsSize;
    private long maxDocuments;

    private boolean rowAttrPersistence;
    private String attrMetadataStore;
    private boolean attrNativeTypes;
    /**
     * Constructor.
     */
    public NGSIMongoSink() {
        super();
    } // NGSIMongoSink
    
    public boolean getRowAttrPersistence() {
        return rowAttrPersistence;
    }
    
    @Override
    public void configure(Context context) {
        collectionsSize = context.getLong("collections_size", 0L);
        
        if ((collectionsSize > 0) && (collectionsSize < 4096)) {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (collections_size="
                    + collectionsSize + ") -- Must be greater than or equal to 4096");
        } else {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (collections_size=" + collectionsSize + ")");
        }  // if else
       
        maxDocuments = context.getLong("max_documents", 0L);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (max_documents=" + maxDocuments + ")");
        
        String attrPersistenceStr = context.getString("attr_persistence", "row");
        
        if (attrPersistenceStr.equals("row") || attrPersistenceStr.equals("column")) {
            rowAttrPersistence = attrPersistenceStr.equals("row");
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_persistence="
                + attrPersistenceStr + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (attr_persistence="
                + attrPersistenceStr + ") must be 'row' or 'column'");
        }  // if else
        
        attrMetadataStore = context.getString("attr_metadata_store", "false");

        if (attrMetadataStore.equals("true") || attrMetadataStore.equals("false")) {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_metadata_store="
                    + attrMetadataStore + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (attr_metadata_store="
                    + attrMetadataStore + ") must be 'true' or 'false'");
        } // if else 

        String attrNativeTypesStr = context.getString("attr_native_types", DEFAULT_ATTR_NATIVE_TYPES);
        if (attrNativeTypesStr.equals("true") || attrNativeTypesStr.equals("false")) {
            attrNativeTypes = Boolean.valueOf(attrNativeTypesStr);
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_native_types=" + attrNativeTypesStr + ")");
        }
        super.configure(context);
    } // configure

    @Override
    void persistBatch(NGSIBatch batch) throws CygnusBadConfiguration, CygnusPersistenceError {
        if (batch == null) {
            LOGGER.debug("[" + this.getName() + "] Null batch, nothing to do");
            return;
        } // if
 
        // Iterate on the destinations
        batch.startIterator();
        
        while (batch.hasNext()) {
            String destination = batch.getNextDestination();
            LOGGER.debug("[" + this.getName() + "] Processing sub-batch regarding the "
                    + destination + " destination");

            // Get the events within the current sub-batch
            ArrayList<NGSIEvent> events = batch.getNextEvents();
            
            // get an aggregator for this destination and initialize it
            NGSIGenericAggregator aggregator = getAggregator(rowAttrPersistence);
            aggregator.initialize(events.get(0));

            for (NGSIEvent event : events) {
                aggregator.aggregate(event);
            } // for
            
            // persist the fieldValues
            persistAggregation(aggregator);
            batch.setNextPersisted(true);
        } // for
    } // persistBatch
    
    @Override
    public void capRecords(NGSIBatch batch, long maxRecords) throws CygnusCappingError {
    } // capRecords

    @Override
    public void expirateRecords(long expirationTime) throws CygnusExpiratingError {
    } // expirateRecords
    
    /**
     * Class for aggregating batches in row mode.
     */
    protected class RowAggregator extends NGSIGenericRowAggregator {

        private String servicePathForNaming;
        private String entityForNaming;
        private String entityType;
        private String attribute;
        private String attributeForNaming;

        /**
         * Instantiates a new Ngsi generic row aggregator.
         *
         * @param enableGrouping     the enable grouping flag for initialization
         * @param enableNameMappings the enable name mappings flag for initialization
         * @param enableEncoding     the enable encoding flag for initialization
         * @param enableGeoParse     the enable geo parse flag for initialization
         */
        protected RowAggregator(boolean enableGrouping, boolean enableNameMappings, boolean enableEncoding, boolean enableGeoParse) {
            super(enableGrouping, enableNameMappings, enableEncoding, enableGeoParse,attrNativeTypes);
        }

        @Override
        public void initialize(NGSIEvent event) throws CygnusBadConfiguration {
            super.initialize(event);
            servicePathForNaming = event.getServicePathForNaming(enableGrouping, enableNameMappings);
            entityForNaming = event.getEntityForNaming(enableGrouping, enableNameMappings, enableEncoding);
            entityType = event.getEntityTypeForNaming(enableGrouping, enableNameMappings);
            attribute = event.getAttributeForNaming(enableNameMappings);
            attributeForNaming = event.getAttributeForNaming(enableNameMappings);
            setDbName(event.getServiceForNaming(enableNameMappings));
            setTableName(buildCollectionName(servicePathForNaming, entityForNaming, attributeForNaming));
        } // initialize

        protected ArrayList<Document> getDocArray() {
            ArrayList<Document> documentAggregation = new ArrayList<>();
            int numEvents = getAggregation().get(NGSIConstants.FIWARE_SERVICE_PATH).size();
            for (int i = 0; i < numEvents; i++) {
                Document doc = new Document(NGSIConstants.RECV_TIME, getAggregation().get(NGSIConstants.RECV_TIME).get(i));
                switch (dataModel) {
                    case DMBYSERVICEPATH:
                        doc.append(NGSIConstants.ENTITY_ID,  getAggregation().get(NGSIConstants.ENTITY_ID).get(i))
                                .append(NGSIConstants.ENTITY_TYPE, getAggregation().get(NGSIConstants.ENTITY_TYPE).get(i))
                                .append(NGSIConstants.ATTR_NAME, getAggregation().get(NGSIConstants.ATTR_NAME).get(i))
                                .append(NGSIConstants.ATTR_TYPE, getAggregation().get(NGSIConstants.ATTR_TYPE).get(i))
                                .append(NGSIConstants.ATTR_VALUE, getAggregation().get(NGSIConstants.ATTR_VALUE).get(i));
                        break;
                    case DMBYENTITY:
                        doc.append(NGSIConstants.ATTR_NAME, getAggregation().get(NGSIConstants.ATTR_NAME).get(i))
                            .append(NGSIConstants.ATTR_TYPE, getAggregation().get(NGSIConstants.ATTR_TYPE).get(i))
                            .append(NGSIConstants.ATTR_VALUE, getAggregation().get(NGSIConstants.ATTR_VALUE).get(i));
                        break;
                    case DMBYATTRIBUTE:
                        doc.append(NGSIConstants.ATTR_TYPE, getAggregation().get(NGSIConstants.ATTR_TYPE).get(i))
                            .append(NGSIConstants.ATTR_VALUE, getAggregation().get(NGSIConstants.ATTR_VALUE).get(i));
                        break;
                    default:
                        return null; // this will never be reached
                } // switch

                if (attrMetadataStore.equalsIgnoreCase("true")) {
                    doc.append(NGSIConstants.ATTR_MD, getAggregation().get(NGSIConstants.ATTR_MD).get(i));
                }
                documentAggregation.add(doc);
            } // for
            return documentAggregation;
        } // getDocArray

    } // RowAggregator
    
    /**
     * Class for aggregating batches in column mode.
     */
    protected class ColumnAggregator extends NGSIGenericColumnAggregator {

        private String servicePathForNaming;
        private String entityForNaming;
        private String entityType;
        private String attribute;
        private String attributeForNaming;

        /**
         * Instantiates a new Ngsi generic column aggregator.
         *
         * @param enableGrouping     the enable grouping
         * @param enableNameMappings the enable name mappings
         * @param enableEncoding     the enable encoding
         * @param enableGeoParse     the enable geo parse
         */
        public ColumnAggregator(boolean enableGrouping, boolean enableNameMappings, boolean enableEncoding, boolean enableGeoParse) {
            super(enableGrouping, enableNameMappings, enableEncoding, enableGeoParse, attrNativeTypes);
        }

        @Override
        public void initialize(NGSIEvent event) throws CygnusBadConfiguration {
            super.initialize(event);
            servicePathForNaming = event.getServicePathForNaming(enableGrouping, enableNameMappings);
            entityForNaming = event.getEntityForNaming(enableGrouping, enableNameMappings, enableEncoding);
            entityType = event.getEntityTypeForNaming(enableGrouping, enableNameMappings);
            attribute = event.getAttributeForNaming(enableNameMappings);
            attributeForNaming = event.getAttributeForNaming(enableNameMappings);
            setDbName(event.getServiceForNaming(enableNameMappings));
            setTableName(buildCollectionName(servicePathForNaming, entityForNaming, attributeForNaming));
        } // initialize

        protected ArrayList<Document> getDocArray() {
            ArrayList<Document> documentAggregation = new ArrayList<>();
            int numEvents = getAggregation().get(NGSIConstants.FIWARE_SERVICE_PATH).size();
            for (int i = 0; i < numEvents; i++) {
                Document doc = new Document(NGSIConstants.RECV_TIME,
                        getStringValueForJsonElement(getAggregation().get(NGSIConstants.RECV_TIME).get(i), QUOTATION_MARK_CHAR));
                doc.append(NGSIConstants.ENTITY_ID,
                        getStringValueForJsonElement(getAggregation().get(NGSIConstants.ENTITY_ID).get(i), QUOTATION_MARK_CHAR))
                        .append(NGSIConstants.ENTITY_TYPE,
                                getStringValueForJsonElement(getAggregation().get(NGSIConstants.ENTITY_TYPE).get(i), QUOTATION_MARK_CHAR));
                Iterator<String> it = aggregation.keySet().iterator();
                while (it.hasNext()) {
                    String entry = (String) it.next();
                    ArrayList<JsonElement> values = (ArrayList<JsonElement>) aggregation.get(entry);
                    JsonElement value = values.get(i);
                    String stringValue = getStringValueForJsonElement(value, QUOTATION_MARK_CHAR);
                    if (stringValue != null && !entry.equals(NGSIConstants.RECV_TIME) && !entry.equals(NGSIConstants.ENTITY_ID) && !entry.equals(NGSIConstants.ENTITY_TYPE)) {
                                doc.append(entry, stringValue);
                    }
                } // while
                if (attrMetadataStore.equalsIgnoreCase("true")) {
                    doc.append(NGSIConstants.ATTR_MD, getAggregation().get(NGSIConstants.ATTR_MD).get(i));
                } // if
                documentAggregation.add(doc);
            } // for
            return documentAggregation;
        } // getDocArray

    } // ColumnAggregator
    
    protected NGSIGenericAggregator getAggregator(boolean rowAttrPersistence) {
        if (rowAttrPersistence) {
            return new RowAggregator(enableGrouping, enableNameMappings, enableEncoding, false);
        } else {
            return new ColumnAggregator(enableGrouping, enableNameMappings, enableEncoding, false);
        } // if else
    } // getAggregator
    
    private void persistAggregation(NGSIGenericAggregator aggregator) throws CygnusPersistenceError {
        ArrayList<Document> aggregation = new ArrayList<>();

        if (rowAttrPersistence && aggregator instanceof RowAggregator) {
            aggregation = ((RowAggregator) aggregator).getDocArray();
        } else {
            aggregation = ((ColumnAggregator) aggregator).getDocArray();
        }
        
        if (aggregation.isEmpty()) {
            return;
        } // if

        
        String dbName = aggregator.getDbName(enableLowercase);
        String collectionName = aggregator.getTableName(enableLowercase);
        LOGGER.info("[" + this.getName() + "] Persisting data at NGSIMongoSink. Database: "
                + dbName + ", Collection: " + collectionName + ", Data: " + aggregation.toString());
        
        try {
            backend.createDatabase(dbName);
            backend.createCollection(dbName, collectionName, collectionsSize, maxDocuments, dataExpiration);
            backend.insertContextDataRaw(dbName, collectionName, aggregation);
        } catch (Exception e) {
            throw new CygnusPersistenceError("-, " + e.getMessage());
        } // try catch
    } // persistAggregation



} // NGSIMongoSink
