package com.telefonica.iot.cygnus.aggregation;

import com.google.gson.JsonElement;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.interceptors.NGSIEvent;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.utils.NGSIConstants;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

/**
 * The type Ngsi generic aggregator.
 */
public abstract class NGSIGenericAggregator {

    // Logger
    private static final CygnusLogger LOGGER = new CygnusLogger(NGSIGenericAggregator.class);

    // Setup for casting to native persistance types
    private boolean attrNativeTypes;
    /**
     * The Enable grouping.
     */
    protected boolean enableGrouping;
    /**
     * The Enable encoding.
     */
    protected boolean enableEncoding;
    /**
     * The Enable name mappings.
     */
    protected boolean enableNameMappings;
    /**
     * The Enable geo parse.
     */
    protected boolean enableGeoParse;

    // Default value for attrNativeTypes
    private static final String DEFAULT_ATTR_NATIVE_TYPES = "false";

    /**
     * The Aggregation.
     */
// object containing the aggregated data
    protected LinkedHashMap<String, ArrayList<JsonElement>> aggregation;

    private String service;
    private String servicePathForData;
    private String servicePathForNaming;
    private String entityForNaming;
    private String entityType;
    private String attribute;
    private String dbName;
    private String tableName;
    protected String orgName;
    protected String pkgName;
    protected String resName;

    /**
     * Instantiates a new Ngsi generic aggregator.
     *
     * @param enableGrouping     the enable grouping
     * @param enableNameMappings the enable name mappings
     * @param enableEncoding     the enable encoding
     * @param enableGeoParse     the enable geo parse
     * @param attrNativeTypes    the attr native types
     */
    NGSIGenericAggregator(boolean enableGrouping, boolean enableNameMappings, boolean enableEncoding, boolean enableGeoParse, boolean attrNativeTypes) {
        this.enableEncoding = enableEncoding;
        this.enableNameMappings = enableNameMappings;
        this.enableEncoding = enableEncoding;
        this.enableGeoParse = enableGeoParse;
        this.attrNativeTypes = attrNativeTypes;
        aggregation = new LinkedHashMap<>();
    } // MySQLAggregator

    /**
     * Gets aggregation.
     *
     * @return the aggregation
     */
    public LinkedHashMap<String, ArrayList<JsonElement>> getAggregation() {
        return aggregation;
    } //getAggregation

    /**
     * Gets service path for data.
     *
     * @return the service path for data
     */
    protected String getServicePathForData() {
        return servicePathForData;
    } //getServicePathForData

    /**
     * Gets db name.
     *
     * @param enableLowercase the enable lowercase
     * @return the db name
     */
    public String getDbName(boolean enableLowercase) {
        if (enableLowercase) {
            return dbName.toLowerCase();
        } else {
            return dbName;
        } // if else
    } // getDbName

    /**
     * Gets table name.
     *
     * @param enableLowercase the enable lowercase
     * @return the table name
     */
    public String getTableName(boolean enableLowercase) {
        if (enableLowercase) {
            return tableName.toLowerCase();
        } else {
            return tableName;
        } // if else
    } // getTableName


    /**
     * Sets db name.
     *
     * @param dbName the db name
     */
    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * Sets table name.
     *
     * @param tableName the table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    /**
     * Gets org name.
     *
     * @return the org name
     */
    public String getOrgName() {
        return orgName;
    }

    /**
     * Sets org name.
     *
     * @param orgName the org name
     */
    public void setOrgName(String orgName) {
        this.orgName = orgName;
    }

    /**
     * Gets pkg name.
     *
     * @return the pkg name
     */
    public String getPkgName() {
        return pkgName;
    }

    /**
     * Sets pkg name.
     *
     * @param pkgName the pkg name
     */
    public void setPkgName(String pkgName) {
        this.pkgName = pkgName;
    }

    /**
     * Gets res name.
     *
     * @return the res name
     */
    public String getResName() {
        return resName;
    }

    /**
     * Sets res name.
     *
     * @param resName the res name
     */
    public void setResName(String resName) {
        this.resName = resName;
    }

    /**
     * Gets string value for json element.
     *
     * @param value the value
     * @return the string value for json element
     */
    public String getStringValueForJsonElement(JsonElement value, String quotationMark) {
        String stringValue;
        if (attrNativeTypes) {
            if (value == null || value.isJsonNull()) {
                stringValue = "NULL";
            } else if (value.isJsonPrimitive()) {
                if (value.getAsJsonPrimitive().isBoolean()) {
                    stringValue = value.getAsString().toUpperCase();
                } else if (value.getAsJsonPrimitive().isNumber()) {
                    stringValue = value.getAsString();
                }else {
                    if (value.toString().contains("ST_GeomFromGeoJSON") || value.toString().contains("ST_SetSRID")) {
                        stringValue = value.getAsString().replace("\\", "");
                    } else {
                        stringValue = quotationMark + value.getAsString() + quotationMark;
                    }
                }
            } else {
                stringValue = quotationMark + value.toString() + quotationMark;
            }
        } else {
            if (value.isJsonPrimitive()) {
                stringValue = quotationMark + value.getAsString() + quotationMark;
            } else {
                stringValue = quotationMark + value.toString() + quotationMark;
            }
        }
        LOGGER.debug("[" + getName() + "] aggregation entry = "  + stringValue);
        return stringValue;
    }

    /**
     * Gets values for insert.
     *
     * @return the values for insert
     */
    public String getValuesForInsert(String openEntity, String closeEntity, String separator, String quotationMark) {
        String valuesForInsert = "";
        int numEvents = aggregation.get(NGSIConstants.FIWARE_SERVICE_PATH).size();

        for (int i = 0; i < numEvents; i++) {
            if (i == 0) {
                valuesForInsert += openEntity;
            } else {
                valuesForInsert += separator + openEntity;
            } // if else
            boolean first = true;
            Iterator<String> it = aggregation.keySet().iterator();
            while (it.hasNext()) {
                String entry = (String) it.next();
                ArrayList<JsonElement> values = (ArrayList<JsonElement>) aggregation.get(entry);
                JsonElement value = values.get(i);
                String stringValue = getStringValueForJsonElement(value, quotationMark);
                if (first) {
                    valuesForInsert += stringValue;
                    first = false;
                } else {
                    valuesForInsert += separator + stringValue;
                } // if else
            } // while
            valuesForInsert += closeEntity;
        } // for
        return valuesForInsert;
    } // getValuesForInsert

    private String getName() {
        return "NGSIUtils.GenericAggregator";
    }

    public String aggregationToJson() {
        String json = "";
        int numEvents = getAggregation().get(NGSIConstants.FIWARE_SERVICE_PATH).size();
        for (int i = 0; i < numEvents; i++) {
            String record = "";
            if (json.isEmpty()) {
                record = "{";
            } else {
                record += "," + record + "{";
            } // if else
            Iterator<String> it = aggregation.keySet().iterator();
            while (it.hasNext()) {
                String entry = (String) it.next();
                ArrayList<JsonElement> values = (ArrayList<JsonElement>) aggregation.get(entry);
                JsonElement value = values.get(i);
                String stringValue = getStringValueForJsonElement(value, "\"");
                if (!record.equals("{")) {
                    record += ",";
                }
                record += entry + " : " + stringValue;
            }
            if (json.isEmpty()) {
                json += record + "}";
            } else {
                json += "," + record + "}";
            } // if else
        }
        return json;
    }

    /**
     * Gets fields for create.
     *
     * @return the fields for create
     */
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

    /**
     * Gets fields for insert.
     *
     * @return the fields for insert
     */
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

    /**
     * Initialize.
     *
     * @param event the event
     * @throws CygnusBadConfiguration the cygnus bad configuration
     */
    public void initialize(NGSIEvent event) throws CygnusBadConfiguration {
        service = event.getServiceForNaming(enableNameMappings);
        servicePathForData = event.getServicePathForData();
        servicePathForNaming = event.getServicePathForNaming(enableGrouping, enableNameMappings);
        entityForNaming = event.getEntityForNaming(enableGrouping, enableNameMappings, enableEncoding);
    } // initialize

    /**
     * Aggregate.
     *
     * @param cygnusEvent the cygnus event
     */
    public abstract void aggregate(NGSIEvent cygnusEvent);

}
