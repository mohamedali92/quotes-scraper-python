ALTER TABLE quotes
ALTER COLUMN quote_url TYPE bigserial;

ALTER TABLE quotes
RENAME quote_url TO id;

ALTER TABLE quotes
ADD COLUMN quote_url text;