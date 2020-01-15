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
import com.google.gson.JsonNull;
import com.google.gson.JsonPrimitive;
import com.telefonica.iot.cygnus.backends.postgresql.PostgreSQLBackendImpl;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElement;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.errors.CygnusBadContextData;
import com.telefonica.iot.cygnus.errors.CygnusCappingError;
import com.telefonica.iot.cygnus.errors.CygnusExpiratingError;
import com.telefonica.iot.cygnus.errors.CygnusPersistenceError;
import com.telefonica.iot.cygnus.errors.CygnusRuntimeError;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.utils.CommonConstants;
import com.telefonica.iot.cygnus.utils.CommonUtils;
import com.telefonica.iot.cygnus.utils.NGSICharsets;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.flume.Context;

/**
 *
 * @author hermanjunge

 Detailed documentation can be found at:
 https://github.com/telefonicaid/fiware-cygnus/blob/master/doc/design/NGSIPostgreSQLSink.md
 */
public class NGSIPostgreSQLSink extends NGSISink {

    private static final String DEFAULT_ROW_ATTR_PERSISTENCE = "row";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_HOST = "localhost";
    private static final String DEFAULT_USER_NAME = "postgres";
    private static final String DEFAULT_DATABASE = "postgres";
    private static final String DEFAULT_ENABLE_CACHE = "false";
    private static final int DEFAULT_MAX_POOL_SIZE = 3;
    private static final String DEFAULT_ATTR_NATIVE_TYPES = "false";

    private static final CygnusLogger LOGGER = new CygnusLogger(NGSIPostgreSQLSink.class);
    private String postgresqlHost;
    private String postgresqlPort;
    private String postgresqlDatabase;
    private String postgresqlUsername;
    private String postgresqlPassword;
    private int maxPoolSize;
    private boolean rowAttrPersistence;
    private PostgreSQLBackendImpl persistenceBackend;
    private boolean enableCache;
    private boolean attrNativeTypes;

    /**
     * Constructor.
     */
    public NGSIPostgreSQLSink() {
        super();
    } // NGSIPostgreSQLSink

    /**
     * Gets the PostgreSQL host. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL host
     */
    protected String getPostgreSQLHost() {
        return postgresqlHost;
    } // getPostgreSQLHost
    
    /**
     * Gets the PostgreSQL cache. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL cache state
     */
    protected boolean getEnableCache() {
        return enableCache;
    } // getPostgreSQLHost

    /**
     * Gets the PostgreSQL port. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL port
     */
    protected String getPostgreSQLPort() {
        return postgresqlPort;
    } // getPostgreSQLPort

    /**
     * Gets the PostgreSQL database. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL database
     */
    protected String getPostgreSQLDatabase() {
        return postgresqlDatabase;
    } // getPostgreSQLDatabase

    /**
     * Gets the PostgreSQL username. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL username
     */
    protected String getPostgreSQLUsername() {
        return postgresqlUsername;
    } // getPostgreSQLUsername

    /**
     * Gets the PostgreSQL password. It is protected due to it is only required for testing purposes.
     * @return The PostgreSQL password
     */
    protected String getPostgreSQLPassword() {
        return postgresqlPassword;
    } // getPostgreSQLPassword

    /**
     * Returns if the attribute persistence is row-based. It is protected due to it is only required for testing
     * purposes.
     * @return True if the attribute persistence is row-based, false otherwise
     */
    protected boolean getRowAttrPersistence() {
        return rowAttrPersistence;
    } // getRowAttrPersistence

    /**
     * Returns if the attribute value will be native or stringfy. It will be stringfy due to backward compatibility
     * purposes.
     * @return True if the attribute value will be native, false otherwise
     */
    protected boolean getNativeAttrTypes() {
        return attrNativeTypes;
    } // attrNativeTypes

    /**
     * Returns the persistence backend. It is protected due to it is only required for testing purposes.
     * @return The persistence backend
     */
    protected PostgreSQLBackendImpl getPersistenceBackend() {
        return persistenceBackend;
    } // getPersistenceBackend

    /**
     * Sets the persistence backend. It is protected due to it is only required for testing purposes.
     * @param persistenceBackend
     */
    protected void setPersistenceBackend(PostgreSQLBackendImpl persistenceBackend) {
        this.persistenceBackend = persistenceBackend;
    } // setPersistenceBackend

    @Override
    public void configure(Context context) {
        // Read NGSISink general configuration
        super.configure(context);
        
        // Impose enable lower case, since PostgreSQL only accepts lower case
        enableLowercase = true;
        
        postgresqlHost = context.getString("postgresql_host", DEFAULT_HOST);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_host=" + postgresqlHost + ")");
        postgresqlPort = context.getString("postgresql_port", DEFAULT_PORT);
        int intPort = Integer.parseInt(postgresqlPort);

        if ((intPort <= 0) || (intPort > 65535)) {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (postgresql_port=" + postgresqlPort + ")"
                    + " -- Must be between 0 and 65535");
        } else {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_port=" + postgresqlPort + ")");
        }  // if else

        postgresqlDatabase = context.getString("postgresql_database", DEFAULT_DATABASE);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_database=" + postgresqlDatabase + ")");
        postgresqlUsername = context.getString("postgresql_username", DEFAULT_USER_NAME);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_username=" + postgresqlUsername + ")");
        // FIXME: postgresqlPassword should be read as a SHA1 and decoded here
        postgresqlPassword = context.getString("postgresql_password", DEFAULT_PASSWORD);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_password=" + postgresqlPassword + ")");

        maxPoolSize = context.getInteger("postgresql_maxPoolSize", DEFAULT_MAX_POOL_SIZE);
        LOGGER.debug("[" + this.getName() + "] Reading configuration (postgresql_maxPoolSize=" + maxPoolSize + ")");

        rowAttrPersistence = context.getString("attr_persistence", DEFAULT_ROW_ATTR_PERSISTENCE).equals("row");
        String persistence = context.getString("attr_persistence", DEFAULT_ROW_ATTR_PERSISTENCE);

        if (persistence.equals("row") || persistence.equals("column")) {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_persistence="
                + persistence + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (attr_persistence="
                + persistence + ") -- Must be 'row' or 'column'");
        }  // if else
                
        String enableCacheStr = context.getString("backend.enable_cache", DEFAULT_ENABLE_CACHE);
        
        if (enableCacheStr.equals("true") || enableCacheStr.equals("false")) {
            enableCache = Boolean.valueOf(enableCacheStr);
            LOGGER.debug("[" + this.getName() + "] Reading configuration (backend.enable_cache=" + enableCache + ")");
        }  else {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (backend.enable_cache="
                + enableCache + ") -- Must be 'true' or 'false'");
        }  // if else

        String attrNativeTypesStr = context.getString("attr_native_types", DEFAULT_ATTR_NATIVE_TYPES);
        if (attrNativeTypesStr.equals("true") || attrNativeTypesStr.equals("false")) {
            attrNativeTypes = Boolean.valueOf(attrNativeTypesStr);
            LOGGER.debug("[" + this.getName() + "] Reading configuration (attr_native_types=" + attrNativeTypes + ")");
        } else {
            invalidConfiguration = true;
            LOGGER.warn("[" + this.getName() + "] Invalid configuration (attr_native_types="
                + attrNativeTypesStr + ") -- Must be 'true' or 'false'");
        } // if else

    } // configure

    @Override
    public void start() {
        try {
            persistenceBackend = new PostgreSQLBackendImpl(postgresqlHost, postgresqlPort, postgresqlDatabase, postgresqlUsername, postgresqlPassword, maxPoolSize);
        } catch (Exception e) {
            LOGGER.error("Error while creating the PostgreSQL persistence backend. Details="
                    + e.getMessage());
        } // try catch

        super.start();
        LOGGER.info("[" + this.getName() + "] Startup completed");
    } // start

    @Override
    void persistBatch(NGSIBatch batch)
        throws CygnusBadConfiguration, CygnusPersistenceError, CygnusRuntimeError, CygnusBadContextData {
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

            // get the sub-batch for this destination
            ArrayList<NGSIEvent> events = batch.getNextEvents();

            // get an aggregator for this destination and initialize it
            PostgreSQLAggregator aggregator = getAggregator(rowAttrPersistence);
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
     * Class for aggregating fieldValues.
     */
    private abstract class PostgreSQLAggregator {

        // object containing the aggregated data
        protected LinkedHashMap<String, ArrayList<JsonElement>> aggregation;

        protected String service;
        protected String servicePathForData;
        protected String servicePathForNaming;
        protected String entityForNaming;
        protected String attributeForNaming;
        protected String schemaName;
        protected String tableName;
        protected String typedFieldNames;
        protected String fieldNames;

        public PostgreSQLAggregator() {
            aggregation = new LinkedHashMap<>();
        } // PostgreSQLAggregator

        public LinkedHashMap<String, ArrayList<JsonElement>> getAggregation() {
            return aggregation;
        } // getAggregation

        public String getSchemaName(boolean enableLowercase) {
            if (enableLowercase) {
                return schemaName.toLowerCase();
            } else {
                return schemaName;
            } // if else
        } // getDbName

        public String getTableName(boolean enableLowercase) {
            if (enableLowercase) {
                return tableName.toLowerCase();
            } else {
                return tableName;
            } // if else
        } // getTableName

        public String getTypedFieldNames() {
            return typedFieldNames;
        } // getTypedFieldNames

        public String getFieldNames() {
            return fieldNames;
        } // getFieldNames

        protected String getServicePathForData() {
            return servicePathForData;
        } //getServicePathForData

        public String getValuesForInsert() {
            String valuesForInsert = "";
            int numEvents = aggregation.get(NGSIConstants.FIWARE_SERVICE_PATH).size();

            for (int i = 0; i < numEvents; i++) {
                if (i == 0) {
                    valuesForInsert += "(";
                } else {
                    valuesForInsert += ",(";
                } // if else

                boolean first = true;
                Iterator<String> it = aggregation.keySet().iterator();

                while (it.hasNext()) {
                    String entry = (String) it.next();
                    ArrayList<JsonElement> values = (ArrayList<JsonElement>) aggregation.get(entry);
                    JsonElement value = values.get(i);
                    String stringValue = null;
                    if (attrNativeTypes && this instanceof ColumnAggregator) {
                        LOGGER.debug("[" + getName() + "] aggregation entry = "  + entry );
                        if (value == null || value.isJsonNull()) {
                            stringValue = "NULL";
                        } else if (value.isJsonPrimitive()) {
                            if (value.getAsJsonPrimitive().isBoolean()) {
                                stringValue = value.getAsString().toUpperCase();
                            } else if (value.getAsJsonPrimitive().isNumber()) {
                                stringValue = value.getAsString();
                            }else {
                                stringValue = "'" + value.getAsString() + "'";
                            }
                        } else {
                            stringValue = "'" + value.toString() + "'";
                        }
                    } else {
                        if (value.isJsonPrimitive()) {
                            stringValue = "'" + value.getAsString() + "'";
                        } else {
                            stringValue = "'" + value.toString() + "'";
                        }
                    }
                    if (first) {
                        valuesForInsert += stringValue;
                        first = false;
                    } else {
                        valuesForInsert += "," + stringValue;
                    } // if else
                } // while

                valuesForInsert += ")";
            } // for

            return valuesForInsert;
        } // getValuesForInsert

        public String getFieldsForCreate() {
            String fieldsForCreate = "(";
            boolean first = true;
            Iterator<String> it = aggregation.keySet().iterator();

            while (it.hasNext()) {
                if (first) {
                    fieldsForCreate += (String) it.next() + " text";
                    first = false;
                } else {
                    fieldsForCreate += "," + (String) it.next() + " text";
                } // if else
            } // while

            return fieldsForCreate + ")";
        } // getFieldsForCreate

        public String getFieldsForInsert() {
            String fieldsForInsert = "(";
            boolean first = true;
            Iterator<String> it = aggregation.keySet().iterator();
            while (it.hasNext()) {
                if (first) {
                    fieldsForInsert += (String) it.next();
                    first = false;
                } else {
                    fieldsForInsert += "," + (String) it.next();
                } // if else
            } // while
            return fieldsForInsert + ")";
        } // getFieldsForInsert

        public void initialize(NGSIEvent event) throws CygnusBadConfiguration {
            service = event.getServiceForNaming(enableNameMappings);
            servicePathForData = event.getServicePathForData();
            servicePathForNaming = event.getServicePathForNaming(enableGrouping, enableNameMappings);
            entityForNaming = event.getEntityForNaming(enableGrouping, enableNameMappings, enableEncoding);
            attributeForNaming = event.getAttributeForNaming(enableNameMappings);
            schemaName = buildSchemaName(service);
            tableName = buildTableName(servicePathForNaming, entityForNaming, attributeForNaming);
        } // initialize

        public abstract void aggregate(NGSIEvent cygnusEvent);

    } // PostgreSQLAggregator

    /**
     * Class for aggregating batches in row mode.
     */
    protected class RowAggregator extends PostgreSQLAggregator {

        @Override
        public void initialize(NGSIEvent cygnusEvent) throws CygnusBadConfiguration {
            super.initialize(cygnusEvent);
            LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();
            aggregation.put(NGSIConstants.RECV_TIME_TS, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.RECV_TIME, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.FIWARE_SERVICE_PATH, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ENTITY_ID, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ENTITY_TYPE, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ATTR_NAME, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ATTR_TYPE, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ATTR_VALUE, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ATTR_MD, new ArrayList<JsonElement>());
        } // initialize

        @Override
        public void aggregate(NGSIEvent event) {
            // get the getRecvTimeTs headers
            long recvTimeTs = event.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);

            // get the getRecvTimeTs body
            ContextElement contextElement = event.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            for (ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                JsonElement attrValue = contextAttribute.getValue();
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");


                // aggregate the attribute information
                LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();
                aggregation.get(NGSIConstants.RECV_TIME_TS).add(new JsonPrimitive(Long.toString(recvTimeTs)));
                aggregation.get(NGSIConstants.RECV_TIME).add(new JsonPrimitive(recvTime));
                aggregation.get(NGSIConstants.FIWARE_SERVICE_PATH).add(new JsonPrimitive(getServicePathForData()));
                aggregation.get(NGSIConstants.ENTITY_ID).add(new JsonPrimitive(entityId));
                aggregation.get(NGSIConstants.ENTITY_TYPE).add(new JsonPrimitive(entityType));
                aggregation.get(NGSIConstants.ATTR_NAME).add(new JsonPrimitive(attrName));
                aggregation.get(NGSIConstants.ATTR_TYPE).add(new JsonPrimitive(attrType));
                aggregation.get(NGSIConstants.ATTR_VALUE).add(attrValue);
                aggregation.get(NGSIConstants.ATTR_MD).add(new JsonPrimitive(attrMetadata));
            } // for
        } // aggregate

    } // RowAggregator

    /**
     * Class for aggregating batches in column mode.
     */
    protected class ColumnAggregator extends PostgreSQLAggregator {

        @Override
        public void initialize(NGSIEvent cygnusEvent) throws CygnusBadConfiguration {
            super.initialize(cygnusEvent);

            // particular initialization
            LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();
            aggregation.put(NGSIConstants.RECV_TIME, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.FIWARE_SERVICE_PATH, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ENTITY_ID, new ArrayList<JsonElement>());
            aggregation.put(NGSIConstants.ENTITY_TYPE, new ArrayList<JsonElement>());

            // iterate on all this context element attributes, if there are attributes
            ArrayList<ContextAttribute> contextAttributes = cygnusEvent.getContextElement().getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                return;
            } // if

            for (ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                aggregation.put(attrName, new ArrayList<JsonElement>());
                aggregation.put(attrName + "_md", new ArrayList<JsonElement>());
            } // for
        } // initialize

        @Override
        public void aggregate(NGSIEvent cygnusEvent) {
            // Number of previous values
            int numPreviousValues = getAggregation().get(NGSIConstants.FIWARE_SERVICE_PATH).size();

            // get the getRecvTimeTs headers
            long recvTimeTs = cygnusEvent.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);

            // get the getRecvTimeTs body
            ContextElement contextElement = cygnusEvent.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            LinkedHashMap<String, ArrayList<JsonElement>> aggregation = getAggregation();
            aggregation.get(NGSIConstants.RECV_TIME).add(new JsonPrimitive(recvTime));
            aggregation.get(NGSIConstants.FIWARE_SERVICE_PATH).add(new JsonPrimitive(getServicePathForData()));
            aggregation.get(NGSIConstants.ENTITY_ID).add(new JsonPrimitive(entityId));
            aggregation.get(NGSIConstants.ENTITY_TYPE).add(new JsonPrimitive(entityType));

            for (ContextAttribute contextAttribute : contextAttributes) {
                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                JsonElement attrValue = contextAttribute.getValue();
                String attrMetadata = contextAttribute.getContextMetadata();
                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
                // Check if the attribute already exists in the form of 2 columns (one for metadata); if not existing,
                // add an empty value for all previous rows

                if (aggregation.containsKey(attrName)) {
                    aggregation.get(attrName).add(attrValue);
                    aggregation.get(attrName + "_md").add(new JsonPrimitive(attrMetadata));
                } else {
                    ArrayList<JsonElement> values = new ArrayList<JsonElement>(Collections.nCopies(numPreviousValues, null));
                    values.add(attrValue);
                    aggregation.put(attrName, values);
                    ArrayList<JsonElement> valuesMd = new ArrayList<JsonElement>(Collections.nCopies(numPreviousValues, null));
                    valuesMd.add(new JsonPrimitive(attrMetadata));
                    aggregation.put(attrName + "_md", valuesMd);
                } // if else
            } // for

            // Iterate on all the aggregations, checking for not updated attributes; add an empty value if missing
            for (String key : aggregation.keySet()) {
                ArrayList<JsonElement> values = aggregation.get(key);

                if (values.size() == numPreviousValues) {
                    values.add(null);
                } // if
            } // for
        } // aggregate

    } // ColumnAggregator

    private PostgreSQLAggregator getAggregator(boolean rowAttrPersistence) {
        if (rowAttrPersistence) {
            return new RowAggregator();
        } else {
            return new ColumnAggregator();
        } // if else
    } // getAggregator

    private void persistAggregation(PostgreSQLAggregator aggregator) throws CygnusPersistenceError, CygnusRuntimeError, CygnusBadContextData {
        String fieldsForCreate = aggregator.getFieldsForCreate();
        String fieldsForInsert = aggregator.getFieldsForInsert();
        String valuesForInsert = aggregator.getValuesForInsert();
        String schemaName = aggregator.getSchemaName(enableLowercase);
        String tableName = aggregator.getTableName(enableLowercase);

        LOGGER.info("[" + this.getName() + "] Persisting data at NGSIPostgreSQLSink. Schema ("
                + schemaName + "), Table (" + tableName + "), Fields (" + fieldsForInsert + "), Values ("
                + valuesForInsert + ")");
        
        try {
            if (aggregator instanceof RowAggregator) {
                persistenceBackend.createSchema(schemaName);
                persistenceBackend.createTable(schemaName, tableName, fieldsForCreate);
            } // if
            // creating the database and the table has only sense if working in row mode, in column node
            // everything must be provisioned in advance

            if (valuesForInsert.equals("")) {
                LOGGER.debug("[" + this.getName() + "] no values for insert");
            } else {
                persistenceBackend.insertContextData(schemaName, tableName, fieldsForInsert, valuesForInsert);
            }
        } catch (Exception e) {
            throw new CygnusPersistenceError("-, " + e.getMessage());
        } // try catch
    } // persistAggregation
    
    /**
     * Creates a PostgreSQL DB name given the FIWARE service.
     * @param service
     * @return The PostgreSQL DB name
     * @throws CygnusBadConfiguration
     */
    public String buildSchemaName(String service) throws CygnusBadConfiguration {
        String name;
        
        if (enableEncoding) {
            name = NGSICharsets.encodePostgreSQL(service);
        } else {
            name = NGSIUtils.encode(service, false, true);
        } // if else

        if (name.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building schema name '" + name
                    + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
        } // if

        return name;
    } // buildSchemaName

    /**
     * Creates a PostgreSQL table name given the FIWARE service path, the entity and the attribute.
     * @param servicePath
     * @param entity
     * @param attribute
     * @return The PostgreSQL table name
     * @throws CygnusBadConfiguration
     */
    public String buildTableName(String servicePath, String entity, String attribute) throws CygnusBadConfiguration {
        String name;

        if (enableEncoding) {
            switch(dataModel) {
                case DMBYSERVICEPATH:
                    name = NGSICharsets.encodePostgreSQL(servicePath);
                    break;
                case DMBYENTITY:
                    name = NGSICharsets.encodePostgreSQL(servicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodePostgreSQL(entity);
                    break;
                case DMBYATTRIBUTE:
                    name = NGSICharsets.encodePostgreSQL(servicePath)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodePostgreSQL(entity)
                            + CommonConstants.CONCATENATOR
                            + NGSICharsets.encodePostgreSQL(attribute);
                    break;
                default:
                    throw new CygnusBadConfiguration("Unknown data model '" + dataModel.toString()
                            + "'. Please, use dm-by-service-path, dm-by-entity or dm-by-attribute");
            } // switch
        } else {
            switch(dataModel) {
                case DMBYSERVICEPATH:
                    if (servicePath.equals("/")) {
                        throw new CygnusBadConfiguration("Default service path '/' cannot be used with "
                                + "dm-by-service-path data model");
                    } // if
                    
                    name = NGSIUtils.encode(servicePath, true, false);
                    break;
                case DMBYENTITY:
                    String truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSIUtils.encode(entity, false, true);
                    break;
                case DMBYATTRIBUTE:
                    truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSIUtils.encode(entity, false, true)
                            + '_' + NGSIUtils.encode(attribute, false, true);
                    break;
                default:
                    throw new CygnusBadConfiguration("Unknown data model '" + dataModel.toString()
                            + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
            } // switch
        } // if else

        if (name.length() > NGSIConstants.POSTGRESQL_MAX_NAME_LEN) {
            throw new CygnusBadConfiguration("Building table name '" + name
                    + "' and its length is greater than " + NGSIConstants.POSTGRESQL_MAX_NAME_LEN);
        } // if

        return name;
    } // buildTableName

} // NGSIPostgreSQLSink
