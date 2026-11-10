
CREATE TABLE IF NOT EXISTS tasks(
    id : UUID PRIMARY KEY,
    type : TEXT NOT NULL,
    payload: JSONB NOT NULL DEFAULT '{}',
    status : TEXT NOT NULL,  -- pending/running/succeded/failed
    error : TEXT,
    created_at: TIMESTAMPZ NOT NULL DEFAULT now(),
    updated_at: TIMESTAMPZ NOT NULL DEFAULT now()
)