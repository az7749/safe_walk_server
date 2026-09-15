ALTER TABLE sos_request_detail
    ALTER COLUMN sent_at DROP NOT NULL,
    ALTER COLUMN read_at DROP NOT NULL;

