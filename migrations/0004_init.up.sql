ALTER TABLE quotes
ALTER COLUMN quote_url DROP DEFAULT;

ALTER TABLE quotes
ADD COLUMN tags_links text[];

