-- Fixed version of create_partition_table_with_indexes.
--
-- The previous version named its indexes after the month only (idx_2024_01_unix_date).
-- Index names are unique per schema, not per table, so the first table to get a partition
-- for a month took the name and, for every later table, CREATE INDEX IF NOT EXISTS found
-- the name taken and silently skipped -- those partitions never got the two indexes.
-- The index names now include the partition name, so each partition gets its own.
--
-- Only partitions created after this replacement are affected; partitions that already
-- exist without these indexes keep lacking them until the indexes are created separately.
--
-- Same caveats as before: search_path must point at the schema (the inner statements are
-- unqualified), and target_table must be lower case (it is quoted by format('%I', ...)).
SET search_path TO algo_oltp, public;

CREATE OR REPLACE PROCEDURE create_partition_table_with_indexes(
    target_table TEXT,
    start_date DATE,
    end_date DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    current_start DATE := date_trunc('month', start_date);
    next_start DATE;
    part_name TEXT;
    idx1_name TEXT;
    idx2_name TEXT;
    create_partition_sql TEXT;
    create_index1_sql TEXT;
    create_index2_sql TEXT;
BEGIN
    WHILE current_start < end_date LOOP
        next_start := current_start + interval '1 month';
        part_name := format('%s_%s', target_table, to_char(current_start, 'YYYY_MM'));
        idx1_name := format('idx_%s_unix_date', part_name);
        idx2_name := format('idx_%s_business_date', part_name);

        -- PostgreSQL silently truncates identifiers to 63 bytes. Two long table names could
        -- then truncate to the same index name and IF NOT EXISTS would skip again, which is
        -- the very bug this version fixes -- so refuse instead of truncating.
        IF octet_length(idx2_name) > 63 THEN
            RAISE EXCEPTION 'Index name % is % bytes, over the 63-byte identifier limit; use a shorter table name',
                idx2_name, octet_length(idx2_name);
        END IF;

        -- CREATE PARTITION
        create_partition_sql := format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
             FOR VALUES FROM (%L) TO (%L);',
            part_name, target_table, current_start, next_start
        );

        -- CREATE INDEX ON unix_date
        create_index1_sql := format(
            'CREATE INDEX IF NOT EXISTS %I ON %I (%I);',
            idx1_name, part_name, 'unix_date'
        );

        -- CREATE INDEX ON business_date
        create_index2_sql := format(
            'CREATE INDEX IF NOT EXISTS %I ON %I (%I);',
            idx2_name, part_name, 'business_date'
        );

        -- Execute
        RAISE NOTICE 'Creating partition: %', part_name;
        EXECUTE create_partition_sql;

        RAISE NOTICE 'Creating index: %', idx1_name;
        EXECUTE create_index1_sql;

        RAISE NOTICE 'Creating index: %', idx2_name;
        EXECUTE create_index2_sql;

        current_start := next_start;
    END LOOP;
END;
$$;
