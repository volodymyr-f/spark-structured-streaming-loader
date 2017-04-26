
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


CREATE TABLE loader.page_count_hourly (
    m_id INT,
    tenant_id INT,
    day DATE,
    hour INT,
    project_name varchar,
    project_type varchar,
    lang varchar,
    page TEXT,
    size INT,
    req_count INT,
    last_read timestamp,
    last_upd timestamp,
    last_commit_message TEXT,    
    PRIMARY KEY(m_id, tenant_id)
);

