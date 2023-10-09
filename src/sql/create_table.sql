DROP TABLE IF EXISTS streaming;
CREATE TABLE IF NOT EXISTS streaming(
    first_name String,
    last_name String,
    email String,
    age int
);
MSCK REPAIR TABLE streaming;