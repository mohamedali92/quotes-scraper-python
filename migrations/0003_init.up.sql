ALTER TABLE quotes
DROP COLUMN quote_url;

ALTER TABLE quotes
RENAME id TO quote_url;

ALTER TABLE quotes
ALTER COLUMN quote_url TYPE text;