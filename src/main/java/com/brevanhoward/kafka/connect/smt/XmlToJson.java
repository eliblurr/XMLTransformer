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
import java.util.SplittableRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
                "Fields containing the custom keys in XML data. Format: '<key-path>[<alias>][<filter-regex>][capture/extract-regex]'")
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
                Map<String, String> parsedKey = parseKey(key);
                Object data = lookup(map, parsedKey.get("lookupKey"), delimiter);
                if (parsedKey.get("filterRegex") != null){
                    data = filterAndRemoveDuplicates(data, parsedKey.get("filterRegex"));
                }

                if (parsedKey.get("extractRegex") != null) {
                    if (data instanceof List<?>) {
                        List <?> tmp = (List) data;
                        if ( tmp.stream().allMatch(item -> item instanceof String) ) {
                            data = tmp.stream().map(item -> extractSubStringFromRegex((String) item, parsedKey.get("extractRegex")))
                                    .filter(item -> item != null).distinct().collect(Collectors.toList());
                        }
                    }
                }

                put(parsedKey.get("id"), data);
            }
            put(xmlDataKey, xmlData);
            put("created", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        }};

        return xmlDataMap;
    }

    private Map<String, String> parseKey(String key){
        String filterRegex = null;
        String extractRegex = null;
        String lookupKey = key;
        String id = key;

        Pattern pattern = Pattern.compile("(^[^\\<]*)(?:\\<([^>]+)\\>)?(?:\\<([^>]+)\\>)?(?:\\<([^>]+)\\>)?");
        Matcher matcher = pattern.matcher(key);

        if (matcher.find()) {
            id = matcher.group(2);
            lookupKey = matcher.group(1);
            filterRegex = matcher.group(3);
            extractRegex = matcher.group(4);
            if (id == null) id = lookupKey;
        }

        String finalFilterRegex = filterRegex;
        String finalExtractRegex = extractRegex;
        String finalLookupKey = lookupKey;
        String finalId = id;

        return new HashMap<String, String>() {{
            put("filterRegex", finalFilterRegex);
            put("extractRegex", finalExtractRegex);
            put("lookupKey", finalLookupKey);
            put("id", finalId);
        }};
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

    private Object filterAndRemoveDuplicates(Object data, String filterValueRegex) {
        if (data instanceof List<?>) {
            ((List<?>) data).removeIf(item -> !(item instanceof String) );
            ((List<?>) data).removeIf(item -> !Pattern.matches(filterValueRegex, (String) item));
        }
        List<String> res = (List) data;
        return res.stream().distinct().collect(Collectors.toList());
    }

    private String extractSubStringFromRegex(String data, String extractRegex) {
        Pattern pattern = Pattern.compile(extractRegex);
        Matcher matcher = pattern.matcher(data);

        if (matcher.find()) { return matcher.group(); }
        return null;
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
