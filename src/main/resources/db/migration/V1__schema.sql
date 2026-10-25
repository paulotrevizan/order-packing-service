create table inbox_message (
    id                 uuid        primary key,
    event_id           uuid        not null,
    order_id           varchar(64) not null,
    type               varchar(20) not null,
    row_number         smallint,
    expected_rows      smallint,
    payload            jsonb       not null,
    status             varchar(20) not null,
    attempts           int         not null default 0,
    next_attempt_at    timestamptz not null,
    label_requested_at timestamptz,
    failure_reason     varchar(40),
    received_at        timestamptz not null,
    updated_at         timestamptz not null,
    constraint inbox_event_id_unique unique (event_id),
    constraint inbox_type_check check (type in ('ROW_COMPLETED', 'ORDER_COMPLETED')),
    constraint inbox_status_check
        check (status in ('RECEIVED', 'WAITING', 'LABEL_REQUESTED', 'PROCESSED', 'FAILED')),
    constraint inbox_row_number_check
        check ((type = 'ROW_COMPLETED') = (row_number is not null)),
    constraint inbox_expected_rows_check
        check ((type = 'ORDER_COMPLETED') = (expected_rows is not null))
);

create unique index inbox_order_row_unique
    on inbox_message (order_id, row_number)
    where type = 'ROW_COMPLETED';

create unique index inbox_order_completed_unique
    on inbox_message (order_id)
    where type = 'ORDER_COMPLETED';

create index inbox_due_order_completed_idx
    on inbox_message (next_attempt_at)
    where type = 'ORDER_COMPLETED' and status in ('RECEIVED', 'WAITING');

create index inbox_label_requested_idx
    on inbox_message (label_requested_at)
    where status = 'LABEL_REQUESTED';

create index inbox_order_id_idx
    on inbox_message (order_id);

create table packed_order (
    id               uuid        primary key,
    order_id         varchar(64) not null,
    carrier_label_id varchar(64),
    packed_at        timestamptz not null,
    labelled_at      timestamptz,
    constraint packed_order_order_id_unique unique (order_id)
);

create table packed_order_item (
    id              uuid        primary key,
    packed_order_id uuid        not null,
    row_number      smallint    not null,
    sku             varchar(64) not null,
    quantity        int         not null,
    constraint packed_order_item_packed_order_fk
        foreign key (packed_order_id) references packed_order (id),
    constraint packed_order_item_quantity_check check (quantity > 0)
);

create index packed_order_item_packed_order_id_idx
    on packed_order_item (packed_order_id);
