package com.brevanhoward.kafka.connect.smt;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class XmlToJsonTest {

    private String xmlData;
    @Mock SourceRecord sourceRecord;
    private Map<String, String> props;
    @Mock XmlToJson<SourceRecord> xform;
    @Mock private final XmlToJson<SourceRecord> xformKey = new XmlToJson.Key<>();
    @Mock private final XmlToJson<SourceRecord> xformValue = new XmlToJson.Value<>();

    @Before
    public void setUp() throws IOException {
        Path path = Paths.get(getClass().getClassLoader().getResource("sample.xml").getFile());
        xmlData = new String(Files.readAllBytes(path));

        props = new HashMap<String, String>() {{
            put("keys", "Customers.Customer.ContactName[ContactName],Customers.Customer.ContactTitle[ContactTitle],Customers.Customer.ContactName");
            put("keys.delimiter.regex", "\\.");
            put("xml.map.key", "blob");
        }};

        xformValue.configure(props);
        xformKey.configure(props);

        sourceRecord = new SourceRecord(null, null, null,
                null, null, xmlData, null, xmlData, null, null);

        xform = Mockito.mock(XmlToJson.class);
        xform.configure(props);
    }

    @Test
    public void xmlToJson_ValidateTransformOutputIsJson() {
        SourceRecord transformedRecord = xformValue.apply(sourceRecord);
        assertEquals(transformedRecord.key(), sourceRecord.key());
        assertFalse(transformedRecord.value()==sourceRecord.value());
        assertTrue(transformedRecord.value() instanceof Map);
        assertTrue(transformedRecord.key() instanceof String);

        transformedRecord = xformKey.apply(sourceRecord);
        assertEquals(transformedRecord.value(), sourceRecord.value());
        assertFalse(transformedRecord.key()==sourceRecord.key());
        assertTrue(transformedRecord.value() instanceof String);
        assertTrue(transformedRecord.key() instanceof Map);
    }

    @Test
    public void xmlToJson_ValidateTransformOutputHasAllKeys() {

        Map<?,?> value = (Map) xformValue.apply(sourceRecord).value();
        assertFalse(value.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(value.containsKey("Customers.Customer.ContactName"));
        assertTrue(value.containsKey("ContactName"));
        assertTrue(value.containsKey("ContactTitle"));
        assertTrue(value.containsKey("blob"));

        Map<?,?> key = (Map) xformKey.apply(sourceRecord).key();
        assertFalse(key.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(key.containsKey("Customers.Customer.ContactName"));
        assertTrue(value.containsKey("ContactName"));
        assertTrue(value.containsKey("ContactTitle"));
        assertTrue(key.containsKey("blob"));
    }

    @Test
    public void config_ValidateKafkaFieldToUse() {
        SourceRecord transformedRecord = xform.apply(sourceRecord);
        verify(xform, times(1)).apply(sourceRecord);
        assertEquals(xformValue.operatingValue(sourceRecord), sourceRecord.value());
        assertEquals(xformKey.operatingValue(sourceRecord), sourceRecord.key());
    }

    @After
    public void tearDown() throws Exception {
        xformValue.close();
        xformKey.close();
    }

}
