DROP TABLE IF EXISTS counts;

CREATE TABLE counts (
    count uuid PRIMARY KEY NOT NULL DEFAULT gen_random_uuid(),
    by_val int8 NOT NULL
);