ALTER TABLE quotes
DROP COLUMN tags_links;

ALTER TABLE quotes
ALTER COLUMN quote_url SET DEFAULT '';