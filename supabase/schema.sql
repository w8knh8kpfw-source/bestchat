-- BestChat private storage schema.
-- The service-role key is used only by the Render server; never expose it in a browser.

create table if not exists public.bot_owner (
  id integer primary key check (id = 1),
  telegram_user_id bigint not null unique,
  enabled boolean not null default false,
  created_at timestamptz not null default now()
);

create table if not exists public.business_messages (
  business_connection_id text not null,
  chat_id bigint not null,
  message_id bigint not null,
  sender text,
  chat_title text,
  text_content text not null default '',
  caption_content text not null default '',
  media jsonb not null default '{}'::jsonb,
  message_date bigint,
  edited_at bigint,
  is_edited boolean not null default false,
  saved_at timestamptz not null default now(),
  primary key (business_connection_id, chat_id, message_id)
);

create table if not exists public.message_edits (
  id bigint generated always as identity primary key,
  business_connection_id text not null,
  chat_id bigint not null,
  message_id bigint not null,
  old_text text not null default '',
  new_text text not null default '',
  edited_at bigint,
  recorded_at timestamptz not null default now()
);

create table if not exists public.deletion_events (
  id bigint generated always as identity primary key,
  business_connection_id text not null,
  chat_id bigint not null,
  message_id bigint not null,
  archived_text text not null default '',
  sender text,
  chat_title text,
  detected_at timestamptz not null default now()
);

alter table public.bot_owner enable row level security;
alter table public.business_messages enable row level security;
alter table public.message_edits enable row level security;
alter table public.deletion_events enable row level security;

-- No anon/authenticated policies: only the server's service-role key can access these tables.
revoke all on public.bot_owner from anon, authenticated;
revoke all on public.business_messages from anon, authenticated;
revoke all on public.message_edits from anon, authenticated;
revoke all on public.deletion_events from anon, authenticated;
