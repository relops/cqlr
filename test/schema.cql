CREATE TABLE tweet
(
    timeline ascii,
    id timeuuid,
    text text,
    PRIMARY KEY (timeline,id)
);

CREATE TABLE sensors
(    
    id int,
    timestamp timestamp,
    temperature float,
    PRIMARY KEY (id)
);

CREATE TABLE calls
(    
    imsi ascii,
    timestamp timestamp,
    duration int,
    carrier ascii,
    charge decimal,
    PRIMARY KEY (imsi, timestamp)
);

CREATE TABLE queue
(    
    id uuid,
    unix bigint,
    usr ascii,
    msg blob,    
    PRIMARY KEY (id)
);
