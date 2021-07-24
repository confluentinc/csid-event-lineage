# csid-data-governance
CSID Data Governance 

### Add a cluster.info file in a ``local`` folder at the root of the project

Sample of cluster.info for CCloud
```
bootstrap.servers=[brokers]
security.protocol=SASL_SSL
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule   required username='[APIKEY]'   password='[APISECRET]';
sasl.mechanism=PLAIN

# Required for correctness in Apache Kafka clients prior to 2.6
client.dns.lookup=use_all_dns_ips

# Required connection configs for Confluent Cloud Schema Registry
schema.registry.url=[SRURL]
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=[SR_APIKEY]:SR_APISECRET
```

Then just compile and play :)