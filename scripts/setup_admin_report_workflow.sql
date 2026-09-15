ALTER TABLE users
ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'user';

ALTER TABLE user_reports
ADD COLUMN IF NOT EXISTS completed_at TIMESTAMP NULL;

CREATE TABLE IF NOT EXISTS report_reviews (
    review_id BIGSERIAL PRIMARY KEY,
    report_id BIGINT NOT NULL
        REFERENCES user_reports(report_id)
        ON DELETE CASCADE,
    action VARCHAR(20) NOT NULL
        CHECK (action IN ('approved', 'rejected', 'completed')),
    reviewed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_report_reviews_report_id
ON report_reviews(report_id);

CREATE INDEX IF NOT EXISTS idx_user_reports_status
ON user_reports(status);

UPDATE users
SET role = 'admin'
WHERE login_id = 'az7749';
