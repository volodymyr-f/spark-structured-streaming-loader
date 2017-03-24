
CREATE KEYSPACE loader
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

CREATE TABLE loader.events_raw (
    m_id INT,
    tenant_id INT,
    prev_link TEXT,
    curr_link TEXT,    
    timestamp_clicked timestamp,
    timestamp_received timestamp,
    message TEXT,
    type TEXT,    
    PRIMARY KEY(m_id, tenant_id)
);

