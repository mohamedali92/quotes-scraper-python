create table if not exists quotes (
  id bigserial primary key,
  created_at timestamp(0) with time zone default now(),
  quote_text text,
  author text,
  tags text[],
  likes bigint,
  quote_url text
);