package store

import (
	"context"
	"github.com/jackc/pgx/v5"
	"time"
	"yap-algorithm/internal/application"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/ranking"
)

type FeedItem struct {
	Position int         `json:"position"`
	Post     domain.Post `json:"post"`
	Reasons  []string    `json:"reasons"`
}
type Page struct {
	SessionID  string     `json:"session_id"`
	Items      []FeedItem `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
	ExpiresAt  time.Time  `json:"expires_at"`
	Model      string     `json:"model"`
}

// Scorer remains an alias while the isolated prototype transport is retained.
type Scorer = application.Scorer

func (s *Store) Build(ctx context.Context, user, tab string, scorer Scorer) (string, error) {
	return (application.FeedBuilder{Repository: s, Scorer: scorer}).Build(ctx, user, tab)
}

func (s *Store) LoadFeed(ctx context.Context, user, tab string) (application.FeedInput, error) {
	var candidates []ranking.Candidate
	var interests []ranking.Interest
	var version int
	err := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		if e := tx.QueryRow(ctx, "select coalesce((select preference_version from recsys.user_state where user_id=$1),1)", user).Scan(&version); e != nil {
			return e
		}
		if tab == "nearby" {
			var count int
			if e := tx.QueryRow(ctx, "select count(*)from recsys.viewer_location()").Scan(&count); e != nil {
				return e
			}
			if count == 0 {
				return domain.ErrInvalid
			}
		}
		var e error
		candidates, e = s.candidates(ctx, tx, tab)
		if e != nil {
			return e
		}
		if tab == "for_you" {
			rows, e := tx.Query(ctx, "select embedding::text,weight,updated_at,negative from recsys.interests where user_id=$1 and model=$2", user, s.Model)
			if e != nil {
				return e
			}
			defer rows.Close()
			for rows.Next() {
				var i ranking.Interest
				var v string
				if e = rows.Scan(&v, &i.Weight, &i.UpdatedAt, &i.Negative); e != nil {
					return e
				}
				i.Vector = domain.ParseVector(v)
				interests = append(interests, i)
			}
			return rows.Err()
		}
		return nil
	})
	if err != nil {
		return application.FeedInput{}, err
	}
	return application.FeedInput{Candidates: candidates, Interests: interests, PreferenceVersion: version}, nil
}

func (s *Store) SaveFeed(ctx context.Context, feed application.PreparedFeed) (string, error) {
	user, tab, model, version, candidates := feed.User, feed.Tab, feed.Model, feed.PreferenceVersion, feed.Candidates
	id := domain.UUID()
	err := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		var latest int
		if e := tx.QueryRow(ctx, "select coalesce((select preference_version from recsys.user_state where user_id=$1),1)", user).Scan(&latest); e != nil {
			return e
		}
		if tab == "for_you" && latest != version {
			return domain.ErrInvalidated
		}
		if _, e := tx.Exec(ctx, "insert into recsys.sessions(id,user_id,tab,preference_version,model)values($1,$2,$3,$4,$5)", id, user, tab, version, model); e != nil {
			return e
		}
		batch := &pgx.Batch{}
		for i, c := range candidates {
			batch.Queue("insert into recsys.session_items(session_id,position,post_id,features,score,reasons,sources)values($1,$2,$3,$4,$5,$6,$7)", id, i, c.ID, c.Features, c.Score, c.Reasons, c.Sources)
		}
		result := tx.SendBatch(ctx, batch)
		return result.Close()
	})
	return id, err
}

func (s *Store) candidates(ctx context.Context, tx pgx.Tx, tab string) ([]ranking.Candidate, error) {
	var query string
	if tab == "for_you" {
		query = `with social as materialized(select author_id from recsys.social_authors()), loc as materialized(select extensions.st_setsrid(extensions.st_makepoint(longitude,latitude),4326)::extensions.geography g from recsys.viewer_location()),
 pool as (
 (select id,'fresh'::text source from feed.posts where created_at>now()-interval '7 days' and author_id<>recsys.viewer()order by created_at desc,id limit 250)
 union all(select p.id,'social'from feed.posts p join social s on s.author_id=p.author_id where p.created_at>now()-interval '7 days'order by p.created_at desc,p.id limit 200)
 union all(select p.id,'nearby'from feed.posts p,loc where p.created_at>now()-interval '7 days'and extensions.st_dwithin(p.location,loc.g,50000)order by p.created_at desc,p.id limit 100)
 union all(select p.id,'popular'from recsys.post_stats s join feed.posts p on p.id=s.post_id where p.created_at>now()-interval '7 days'order by (s.likes+2*s.reposts+1)::float8/(s.impressions+20)desc,p.id limit 150)
 union all(select p.id,'new_author'from feed.posts p left join recsys.post_stats s on s.post_id=p.id where p.created_at>now()-interval '7 days'and coalesce(s.impressions,0)<20 order by p.created_at desc,p.id limit 150)
 union all(select e.post_id,'semantic'from recsys.interests i cross join lateral(select e.post_id from recsys.post_embeddings e where e.model=$1 order by e.embedding operator(extensions.<=>)i.embedding limit 100)e where i.user_id=recsys.viewer()and not i.negative and i.model=$1)
 ), ids as(select id,array_agg(distinct source) sources from pool group by id)
 select p.id::text,p.author_id::text,p.created_at,coalesce(e.embedding::text,''),ids.sources,
 exists(select 1 from social where author_id=p.author_id),exists(select 1 from loc where extensions.st_dwithin(p.location,loc.g,50000)),
 coalesce(st.impressions,0),coalesce(st.likes,0),coalesce(st.reposts,0),seen.user_id is not null
 from ids join feed.posts p on p.id=ids.id left join recsys.post_embeddings e on e.post_id=p.id and e.post_version=p.version and e.model=$1
 left join recsys.post_stats st on st.post_id=p.id left join recsys.seen seen on seen.user_id=recsys.viewer()and seen.post_id=p.id
 where p.author_id<>recsys.viewer()and p.created_at>now()-interval '7 days'
 and not exists(select 1 from recsys.hidden_authors h where h.user_id=recsys.viewer()and h.author_id=p.author_id)
 and not exists(select 1 from recsys.suppressed_posts h where h.user_id=recsys.viewer()and h.post_id=p.id)
 order by p.id limit 1200`
	} else if tab == "friends" {
		query = `select p.id::text,p.author_id::text,p.created_at,''::text,array['friends']::text[],true,false,0,0,0,false from feed.posts p where p.author_id in(select recsys.mutual_authors())order by p.created_at desc,p.id limit 800`
	} else if tab == "nearby" {
		query = `select p.id::text,p.author_id::text,p.created_at,''::text,array['nearby']::text[],false,true,0,0,0,false from feed.posts p,recsys.viewer_location()l where extensions.st_dwithin(p.location,extensions.st_setsrid(extensions.st_makepoint(l.longitude,l.latitude),4326)::extensions.geography,50000)order by p.created_at desc,p.id limit 800`
	} else {
		return nil, domain.ErrInvalid
	}
	var rows pgx.Rows
	var e error
	if tab == "for_you" {
		rows, e = tx.Query(ctx, query, s.Model)
	} else {
		rows, e = tx.Query(ctx, query)
	}
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []ranking.Candidate{}
	for rows.Next() {
		var c ranking.Candidate
		var vector string
		if e = rows.Scan(&c.ID, &c.AuthorID, &c.CreatedAt, &vector, &c.Sources, &c.Social, &c.Nearby, &c.Impressions, &c.Likes, &c.Reposts, &c.Seen); e != nil {
			return nil, e
		}
		if vector != "" {
			c.Embedding = domain.ParseVector(vector)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) Page(ctx context.Context, user, tab, session string, offset, limit int) (Page, int, error) {
	page := Page{SessionID: session, Items: []FeedItem{}}
	next := -1
	err := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		var ownerTab string
		var version, current int
		if e := tx.QueryRow(ctx, `select tab,expires_at,preference_version,model,coalesce((select preference_version from recsys.user_state where user_id=$2),1)from recsys.sessions where id=$1 and user_id=$2`, session, user).Scan(&ownerTab, &page.ExpiresAt, &version, &page.Model, &current); e != nil {
			return normalize(e)
		}
		if ownerTab != tab {
			return domain.ErrInvalid
		}
		if !page.ExpiresAt.After(time.Now()) {
			return domain.ErrExpired
		}
		if tab == "for_you" && version != current {
			return domain.ErrInvalidated
		}
		rows, e := tx.Query(ctx, "select position,post_id::text,reasons from recsys.session_items where session_id=$1 and position>=$2 order by position limit 1200", session, offset)
		if e != nil {
			return e
		}
		type row struct {
			pos     int
			id      string
			reasons []string
		}
		items := []row{}
		ids := []string{}
		for rows.Next() {
			var r row
			if e = rows.Scan(&r.pos, &r.id, &r.reasons); e != nil {
				rows.Close()
				return e
			}
			items = append(items, r)
			ids = append(ids, r.id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		posts, e := loadPosts(ctx, tx, ids, tab)
		if e != nil {
			return e
		}
		served := []int{}
		for index, r := range items {
			p, ok := posts[r.id]
			if !ok {
				continue
			}
			page.Items = append(page.Items, FeedItem{r.pos, p, r.reasons})
			served = append(served, r.pos)
			if len(page.Items) == limit {
				if index+1 < len(items) {
					next = r.pos + 1
				}
				break
			}
		}
		if len(served) > 0 {
			_, e = tx.Exec(ctx, "update recsys.session_items set served_at=coalesce(served_at,now())where session_id=$1 and position=any($2::int[])", session, served)
		}
		return e
	})
	return page, next, err
}
