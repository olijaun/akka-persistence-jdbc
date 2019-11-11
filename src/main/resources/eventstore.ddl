create table event (
    stream varchar(255),
    seq_number long,
    event_type varchar(255),
    tags varchar(255),
    metadata varchar(255),
    event_data varchar(255),
    deleted boolean,
);