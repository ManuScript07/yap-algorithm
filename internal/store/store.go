package store

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"slices"
	"time"
	"yap-algorithm/internal/domain"
)

type Store struct {
	Pool  *pgxpool.Pool
	Model string
}

func Open(ctx context.Context, url, model string) (*Store, error) {
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		return nil, e
	}
	cfg.MaxConns = 12
	cfg.MinConns = 1
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, e := c.Exec(ctx, "set statement_timeout='8s'; set lock_timeout='3s'; set search_path=pg_catalog,extensions")
		return e
	}
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, e
	}
	return &Store{p, model}, nil
}
func (s *Store) WithUser(ctx context.Context, user string, fn func(pgx.Tx) error) error {
	if !domain.UUIDValid(user) {
		return domain.ErrForbidden
	}
	tx, e := s.Pool.BeginTx(ctx, pgx.TxOptions{})
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, "select set_config('yap.viewer_id',$1,true)", user); e != nil {
		return e
	}
	var allowed bool
	if e = tx.QueryRow(ctx, "select recsys.account_allowed($1::uuid)", user).Scan(&allowed); e != nil {
		return e
	}
	if !allowed {
		return domain.ErrForbidden
	}
	if e = fn(tx); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) Fixture(ctx context.Context) bool {
	var ok bool
	return s.Pool.QueryRow(ctx, "select recsys.is_fixture()").Scan(&ok) == nil && ok
}
func normalize(e error) error {
	if errors.Is(e, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return e
}
func (s *Store) CreatePost(ctx context.Context, user string, input domain.NewPost) (domain.Post, error) {
	var p domain.Post
	if e := input.Validate(); e != nil {
		return p, e
	}
	id := input.ID
	if id == "" {
		id = domain.UUID()
	}
	if input.Photos == nil {
		input.Photos = []string{}
	}
	err := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		for _, path := range input.Photos {
			if len(path) < len(user)+1 || path[:len(user)+1] != user+"/" {
				return domain.ErrInvalid
			}
		}
		var lat, lon *float64
		if input.Location != nil {
			lat = &input.Location.Latitude
			lon = &input.Location.Longitude
		}
		tag, e := tx.Exec(ctx, `insert into feed.posts(id,author_id,text,audience,language,photos,location)values($1,$2,$3,$4,$5,$6,case when $7::float8 is null then null else extensions.st_setsrid(extensions.st_makepoint($8,$7),4326)::extensions.geography end)on conflict(id)do nothing`, id, user, input.Text, input.Audience, input.Language, input.Photos, lat, lon)
		if e != nil {
			return e
		}
		posts, e := loadPosts(ctx, tx, []string{id}, "")
		if e == nil {
			p = posts[id]
			if p.ID == "" {
				return domain.ErrInvalidated
			}
			if tag.RowsAffected() == 0 {
				sameLocation := (p.Location == nil && input.Location == nil) || (p.Location != nil && input.Location != nil && *p.Location == *input.Location)
				if p.AuthorID != user || p.Text != input.Text || p.Audience != input.Audience || p.Language != input.Language || !slices.Equal(p.Photos, input.Photos) || !sameLocation {
					return domain.ErrInvalidated
				}
			}
		}
		return e
	})
	return p, err
}
func (s *Store) EditPost(ctx context.Context, user, id, text string, version int) error {
	if len([]rune(text)) > domain.MaxPostTextChars || version < 1 {
		return domain.ErrInvalid
	}
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		tag, e := tx.Exec(ctx, "update feed.posts set text=$3,version=version+1,updated_at=now()where id=$1 and author_id=$2 and version=$4", id, user, text, version)
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrInvalidated
		}
		return nil
	})
}
func (s *Store) DeletePost(ctx context.Context, user, id string) error {
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		var deleted bool
		e := tx.QueryRow(ctx, "select recsys.delete_own_post($1)", id).Scan(&deleted)
		if e == nil && !deleted {
			return domain.ErrNotFound
		}
		return e
	})
}
func (s *Store) GetPost(ctx context.Context, user, id string) (domain.Post, error) {
	var p domain.Post
	e := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		posts, e := loadPosts(ctx, tx, []string{id}, "")
		if e != nil {
			return e
		}
		var ok bool
		p, ok = posts[id]
		if !ok {
			return domain.ErrNotFound
		}
		return nil
	})
	return p, e
}
func loadPosts(ctx context.Context, tx pgx.Tx, ids []string, tab string) (map[string]domain.Post, error) {
	rows, e := tx.Query(ctx, `select p.id::text,p.author_id::text,p.text,p.audience,p.language,p.photos,p.version,p.created_at,p.updated_at,
 case when recsys.precise_allowed(recsys.viewer(),p.author_id)then extensions.st_y(p.location::extensions.geometry)end,
 case when recsys.precise_allowed(recsys.viewer(),p.author_id)then extensions.st_x(p.location::extensions.geometry)end
 from feed.posts p where p.id=any($1::uuid[])
 and($2<>'friends'or p.author_id in(select recsys.mutual_authors()))
 and($2<>'for_you'or(not exists(select 1 from recsys.hidden_authors where user_id=recsys.viewer()and author_id=p.author_id)
 and not exists(select 1 from recsys.suppressed_posts where user_id=recsys.viewer()and post_id=p.id)))`, ids, tab)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]domain.Post{}
	for rows.Next() {
		var p domain.Post
		var lat, lon *float64
		if e = rows.Scan(&p.ID, &p.AuthorID, &p.Text, &p.Audience, &p.Language, &p.Photos, &p.Version, &p.CreatedAt, &p.UpdatedAt, &lat, &lon); e != nil {
			return nil, e
		}
		if p.Photos == nil {
			p.Photos = []string{}
		}
		if lat != nil && lon != nil {
			p.Location = &domain.Location{Latitude: *lat, Longitude: *lon}
		}
		out[p.ID] = p
	}
	return out, rows.Err()
}
func (s *Store) Follow(ctx context.Context, user, author string, follow bool) error {
	if user == author || !domain.UUIDValid(author) {
		return domain.ErrInvalid
	}
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		if follow {
			_, e := tx.Exec(ctx, "insert into feed.follows(user_id,author_id)values($1,$2)on conflict do nothing", user, author)
			return e
		}
		_, e := tx.Exec(ctx, "delete from feed.follows where user_id=$1 and author_id=$2", user, author)
		return e
	})
}
func (s *Store) HideAuthor(ctx context.Context, user, author string, hide bool) error {
	if !domain.UUIDValid(author) || author == user {
		return domain.ErrInvalid
	}
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		var e error
		if hide {
			_, e = tx.Exec(ctx, "insert into recsys.hidden_authors values($1,$2)on conflict do nothing", user, author)
		} else {
			_, e = tx.Exec(ctx, "delete from recsys.hidden_authors where user_id=$1 and author_id=$2", user, author)
		}
		if e != nil {
			return e
		}
		return bump(ctx, tx, user, false)
	})
}
func bump(ctx context.Context, tx pgx.Tx, user string, reset bool) error {
	_, e := tx.Exec(ctx, `insert into recsys.user_state(user_id,preference_version,reset_at)values($1,2,case when $2 then clock_timestamp()else '-infinity'::timestamptz end)on conflict(user_id)do update set preference_version=recsys.user_state.preference_version+1,reset_at=case when $2 then clock_timestamp()else recsys.user_state.reset_at end`, user, reset)
	return e
}
func (s *Store) Reset(ctx context.Context, user string) error {
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,0))", user); e != nil {
			return e
		}
		if e := bump(ctx, tx, user, true); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, "delete from recsys.interests where user_id=$1", user); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, "delete from recsys.suppressed_posts where user_id=$1", user)
		return e
	})
}

func sessionSurface(ctx context.Context, tx pgx.Tx, user, session, post string, position int) (string, error) {
	var surface string
	e := tx.QueryRow(ctx, `select s.tab from recsys.sessions s join recsys.session_items i on i.session_id=s.id where s.id=$1 and s.user_id=$2 and s.expires_at>now()and i.position=$3 and i.post_id=$4 and i.served_at is not null`, session, user, position, post).Scan(&surface)
	if e != nil {
		return "", domain.ErrInvalid
	}
	return surface, nil
}
func (s *Store) Events(ctx context.Context, user string, events []domain.Event) error {
	if len(events) < 1 || len(events) > 100 {
		return domain.ErrInvalid
	}
	for _, e := range events {
		if e.Validate() != nil {
			return domain.ErrInvalid
		}
	}
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		for _, event := range events {
			surface, e := sessionSurface(ctx, tx, user, event.SessionID, event.PostID, event.Position)
			if e != nil {
				return e
			}
			var exists bool
			if e = tx.QueryRow(ctx, "select exists(select 1 from feed.posts where id=$1)", event.PostID).Scan(&exists); e != nil {
				return e
			}
			if !exists {
				return domain.ErrForbidden
			}
			var elapsed int
			if e = tx.QueryRow(ctx, "select greatest(0,(extract(epoch from(clock_timestamp()-served_at))*1000)::int)from recsys.session_items where session_id=$1 and position=$2", event.SessionID, event.Position).Scan(&elapsed); e != nil {
				return e
			}
			if event.ActiveMS > elapsed+1500 || event.VisibleMS > elapsed+1500 {
				return domain.ErrInvalid
			}
			if event.Kind == "dwell" {
				var impressed bool
				if e = tx.QueryRow(ctx, "select exists(select 1 from feed.events where user_id=$1 and session_id=$2 and position=$3 and kind='impression')", user, event.SessionID, event.Position).Scan(&impressed); e != nil {
					return e
				}
				if !impressed {
					return domain.ErrInvalid
				}
			}
			_, e = tx.Exec(ctx, `insert into feed.events(id,user_id,post_id,kind,surface,session_id,position,active_ms)values($1,$2,$3,$4,$5,$6,$7,$8)on conflict do nothing`, event.ID, user, event.PostID, event.Kind, surface, event.SessionID, event.Position, event.ActiveMS)
			if e != nil {
				return e
			}
		}
		return nil
	})
}

// Authoritative mutations emit feedback only when state actually changes.
func (s *Store) Action(ctx context.Context, user, post, kind, session string, position int) error {
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		var exists bool
		if e := tx.QueryRow(ctx, "select exists(select 1 from feed.posts where id=$1)", post).Scan(&exists); e != nil {
			return e
		}
		if !exists {
			return domain.ErrNotFound
		}
		surface := "direct"
		if session != "" {
			var e error
			surface, e = sessionSurface(ctx, tx, user, session, post, position)
			if e != nil {
				return e
			}
		}
		if _, e := tx.Exec(ctx, "select set_config('yap.surface',$1,true)", surface); e != nil {
			return e
		}
		var tag int64
		switch kind {
		case "like", "repost":
			table := "feed.likes"
			if kind == "repost" {
				table = "feed.reposts"
			}
			t, e := tx.Exec(ctx, "insert into "+table+"(user_id,post_id)values($1,$2)on conflict do nothing", user, post)
			if e != nil {
				return e
			}
			tag = t.RowsAffected()
		case "unlike", "unrepost":
			table := "feed.likes"
			if kind == "unrepost" {
				table = "feed.reposts"
			}
			t, e := tx.Exec(ctx, "delete from "+table+" where user_id=$1 and post_id=$2", user, post)
			if e != nil {
				return e
			}
			tag = t.RowsAffected()
		case "not_interested":
			t, e := tx.Exec(ctx, "insert into recsys.suppressed_posts values($1,$2)on conflict do nothing", user, post)
			if e != nil {
				return e
			}
			tag = t.RowsAffected()
			if tag > 0 {
				if e = bump(ctx, tx, user, false); e != nil {
					return e
				}
			}
		default:
			return domain.ErrInvalid
		}
		if tag == 0 {
			return nil
		}
		if kind == "not_interested" {
			_, e := tx.Exec(ctx, "insert into feed.events(id,user_id,post_id,kind,surface)values($1,$2,$3,$4,$5)", domain.UUID(), user, post, kind, surface)
			return e
		}
		return nil
	})
}

func Deadline(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 10*time.Second)
}
