package com.brevanhoward.kafka.connect.smt;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class XmlToJson<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Generate Json from XML blob with and XML field keys";

    private interface ConfigName { // define constants for configuration keys
        String KEY_FIELDS_CONFIG = "keys"; // config param to hold keys containing the keys in XML data to extract
        String KEY_DELIMITER_CONFIG = "keys.delimiter.regex"; // config param to specify a single key nested separator
        String XML_MAP_KEY_CONFIG = "xml.map.key"; // config param to specify key in resulting map to hold original xml
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef() // define configuration schema to specify the expected configuration parameters.
        .define(ConfigName.XML_MAP_KEY_CONFIG, ConfigDef.Type.STRING, "_xml_data_", ConfigDef.Importance.MEDIUM,
                "Field containing key in resulting map to hold original xml.")
        .define(ConfigName.KEY_FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH,
                "Fields containing the custom keys in XML data.")
        .define(ConfigName.KEY_DELIMITER_CONFIG, ConfigDef.Type.STRING, ".", ConfigDef.Importance.HIGH,
                "Field containing single nested key delimiter.");

    private static final String PURPOSE = "extracting and converting xml payload to JSON";

    private static final String HEADER_REGEX = "<\\?xml[^>]*\\?>";
    private String xmlDataKey; // key in resulting map to hold original xml.
    private String keyFieldDelimiter; // instance variable to store "keys.delimiter" configured values.
    private List<String> keyFieldNames; // instance variable to store "keys" configured values.

    @Override
    public R apply(R record) {
        Map<String, Object> xmlDataMap = xmlToMap(operatingValue(record), keyFieldNames, keyFieldDelimiter, xmlDataKey);
        return newRecord(record, null, xmlDataMap);
    }

    private Map<String, Object> xmlToMap (Object xmlData, List<String> keysToExtract, String delimiter, String xmlDataKey) {
        XmlMapper xmlMapper = new XmlMapper();
        Map<String, Object> map;

        try {
            String data = (String) xmlData;
            data = data.replaceAll(HEADER_REGEX, "");
            map = xmlMapper.readValue(data, Map.class);
        } catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage());
        }

        Map<String, Object> xmlDataMap = new HashMap<String, Object>() {{
            for (String key : keysToExtract){
                put(key, lookup(map, key, delimiter));
            }
            put(xmlDataKey, xmlData);
            put("created", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }};

        return xmlDataMap;
    }

    private Object lookup(Map<String, Object> nestedMap, String keys, String delimiter) {
        String[] keyArray = keys.split(delimiter);

        Object result = nestedMap;
        for (String key : keyArray) {
            if (result instanceof List) {
                List<?> list = (List<?>) result;
                result = list.stream()
                        .filter(element -> element instanceof Map)
                        .map(element -> ((Map<?, ?>) element).get(key))
                        .filter(value -> value != null).collect(Collectors.toList());
//                        .toList();
            } else if (result instanceof Map) {
                result = ((Map<?, ?>) result).get(key);
                if (result == null) {
                    result = new HashMap<>();
                    break;
                }
            } else {
                // Key not found or not a map or list, return an empty list
                result = new HashMap<>();
                break;
            }
        }
        return result;
    }

    @Override
    public ConfigDef config() { // returns the configuration definition, allowing Kafka Connect to validate the provided configuration.
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> props) { // initialize the transformation with the provided configuration.
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        keyFieldDelimiter = config.getString(ConfigName.KEY_DELIMITER_CONFIG);
        xmlDataKey = config.getString(ConfigName.XML_MAP_KEY_CONFIG);
        keyFieldNames = config.getList(ConfigName.KEY_FIELDS_CONFIG);
    }

    @Override
    public void close() {
        // called when the transformation is no longer needed, releases any resources after transformation is complete
        // No resources to release
    }

    // abstract method that should be implemented by subclasses
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);
    public static class Key<R extends ConnectRecord<R>> extends XmlToJson<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

//        record.topic(), record.kafkaPartition(), Schema.STRING_SCHEMA,
//                customKeys.stream().collect(Collectors.joining(",")),
//                record.valueSchema(), jsonData, record.timestamp()

    }
    public static class Value<R extends ConnectRecord<R>> extends XmlToJson<R> {

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
