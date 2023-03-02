CREATE TYPE apply_status AS ENUM ('ok', 'failed', 'pending', 'height_missing');

CREATE TABLE public.epochs (
    height bigint NOT NULL,
    id text UNIQUE NOT NULL PRIMARY KEY,
    start_height bigint UNIQUE NOT NULL,
    end_height bigint UNIQUE NOT NULL,
    chunks_applied bigint NOT NULL,
    chunk_application_failures bigint NOT NULL
);

CREATE INDEX height ON epochs (height);
CREATE INDEX chunks_applied ON epochs (chunks_applied);

-- the point of this is just to store the smaller 4 bytes in each row of applied_chunks
CREATE TABLE public.neard_versions (
    hash integer NOT NULL PRIMARY KEY,
    version text NOT NULL
);

CREATE TABLE public.applied_chunks (
    height bigint NOT NULL,
    shard_id integer NOT NULL,
    epoch_id text NOT NULL REFERENCES public.epochs (id),
    neard_version_hash integer NOT NULL REFERENCES public.neard_versions (hash),
    status apply_status NOT NULL,
    job_queued_at timestamp NOT NULL,
    PRIMARY KEY (height, shard_id, neard_version_hash)
);

CREATE INDEX block_height ON applied_chunks (height);
CREATE INDEX epoch_id ON applied_chunks (epoch_id);
CREATE INDEX apply_status ON applied_chunks (status);