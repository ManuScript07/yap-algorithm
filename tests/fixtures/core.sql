-- Compatibility fixture ONLY for the isolated local database. Never deploy to YapChat.
create schema if not exists private;
create table public.profiles(id uuid primary key, onboarding_completed boolean not null default true);
create table public.friendships(user_one_id uuid not null references public.profiles, user_two_id uuid not null references public.profiles, primary key(user_one_id,user_two_id),check(user_one_id<user_two_id));
create table public.user_blocks(blocker_user_id uuid not null references public.profiles,blocked_user_id uuid not null references public.profiles,primary key(blocker_user_id,blocked_user_id));
create table public.user_locations(user_id uuid primary key references public.profiles,latitude double precision,longitude double precision,updated_at timestamptz not null default now());
create table private.search_privacy_settings(user_id uuid primary key references public.profiles,share_precise_location boolean not null default false);
create table private.precise_location_exclusions(owner_user_id uuid not null,viewer_user_id uuid not null,primary key(owner_user_id,viewer_user_id));
create table private.fixture_account_status(user_id uuid primary key references public.profiles,banned boolean not null default false,pending_deletion boolean not null default false);
create function private.is_account_globally_banned(u uuid)returns boolean language sql stable as $$select coalesce((select banned from private.fixture_account_status where user_id=u),false)$$;
create function private.is_account_pending_deletion(u uuid)returns boolean language sql stable as $$select coalesce((select pending_deletion from private.fixture_account_status where user_id=u),false)$$;
create function private.is_user_pair_blocked_impl(a uuid,b uuid)returns boolean language sql stable as $$select exists(select 1 from public.user_blocks where (blocker_user_id=a and blocked_user_id=b)or(blocker_user_id=b and blocked_user_id=a))$$;
create table private.yap_algorithm_fixture(singleton boolean primary key default true check(singleton));
insert into private.yap_algorithm_fixture values(true);
