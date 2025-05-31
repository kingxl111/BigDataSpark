-- Загрузка данных в промежуточную таблицу staging.mock_data
SET search_path = staging;

COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA.csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (1).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (2).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (3).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (4).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (5).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (6).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (7).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (8).csv' DELIMITER ',' CSV HEADER;
COPY staging.mock_data FROM '/docker-entrypoint-initdb.d/data/MOCK_DATA (9).csv' DELIMITER ',' CSV HEADER;