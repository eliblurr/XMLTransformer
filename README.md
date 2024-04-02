## Xml To Json Custom Transformer 

Kafka Connect SMT to parse Read Specific XML keys and parse to JSON.

Only supports schemaless
Properties:

|Name| Description                                                                              |Type| Default | Importance   |
|---|------------------------------------------------------------------------------------------|---|-----|--------------|
|`keys`| Comma separated string of list of keys to extract, add alias for key field in box braces | String | | High         |
|`keys.delimiter.regex`| Delimiter string to a single key provided for nested key                                 | String | | High         |
|`xml.map.key`| Field name is resulting json to hold original xml                                        | String | String | `_xml_blob_` | High |



Example on how to add to your connector:
```
transforms=xmltojson
transforms.xmltojson.type=com.brevanhoward.kafka.connect.smt.XmlToJson$Value
transforms.xmltojson.keys.delimiter.regex=\\.
transforms.xmltojson.xml.map.key=xmlBlob
transforms.xmltojson.keys=Customers.Customer.ContactName<AliasForContactName>,Customers.Customer.ContactName, Customers.Customer.ContactTitle
```

ToDO
* ~~add support for records with schemas~~