ALTER TABLE submissions ADD COLUMN resolved_profile_config_json TEXT;
ALTER TABLE submissions ADD COLUMN resolved_policy_config_json TEXT;
ALTER TABLE submissions ADD COLUMN resolved_routes_json TEXT;

ALTER TABLE attempts ADD COLUMN capacity_held INTEGER NOT NULL DEFAULT 1;
UPDATE attempts
SET capacity_held = 0
WHERE state IN ('completed', 'failed', 'cancelled');
