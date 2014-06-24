CREATE TABLE tweet
(
    timeline ascii,
    id timeuuid,
    text text,
    PRIMARY KEY (timeline,id)
);
