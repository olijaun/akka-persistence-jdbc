create table event (
    seq long auto_increment,
    stream varchar(255),
    stream_seq_number long,
    event_type varchar(255),
    tags varchar(255),
    metadata varchar(255),
    event_data varchar(255),
    deleted boolean,
);