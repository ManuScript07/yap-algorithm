package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"math"
	"time"
	"yap-algorithm/internal/domain"
	"yap-algorithm/internal/store"
)

type Embedder interface {
	Embed(context.Context, string) (string, error)
}
type Worker struct {
	Store *store.Store
	Model Embedder
}

// Reindex is an explicit model-rollout/backfill operation. Active leases are
// preserved; event jobs are never reset, and no user feedback is fabricated.
func (w *Worker) Reindex(ctx context.Context) (int64, error) {
	tag, e := w.Store.Pool.Exec(ctx, `insert into recsys.jobs(kind,target_id,version)select 'post',p.id,p.version from feed.posts p left join recsys.post_embeddings em on em.post_id=p.id where p.deleted_at is null and p.moderation_status='allowed'and p.text<>''and(em.post_id is null or em.post_version<>p.version or em.model<>$1)on conflict(kind,target_id,version)do update set done_at=null,attempts=0,available_at=now(),lease=null,locked_until=null where(recsys.jobs.done_at is not null or recsys.jobs.attempts>=8)and(recsys.jobs.locked_until is null or recsys.jobs.locked_until<now())`, w.Store.Model)
	return tag.RowsAffected(), e
}

type job struct {
	ID                  int64
	Kind, Target, Lease string
	EmbeddingPost       string
	Version, Attempts   int
}

func (w *Worker) Once(ctx context.Context) (bool, error) {
	var j job
	e := w.Store.Pool.QueryRow(ctx, `with claim as(select id from recsys.jobs where done_at is null and available_at<=now()and(locked_until is null or locked_until<now())and attempts<8 order by case when kind='event'then 0 else 1 end,available_at,id for update skip locked limit 1)update recsys.jobs j set lease=$1,locked_until=now()+interval '45 seconds',attempts=attempts+1 from claim where j.id=claim.id returning j.id,j.kind,j.target_id::text,j.version,j.lease::text,j.attempts`, domain.UUID()).Scan(&j.ID, &j.Kind, &j.Target, &j.Version, &j.Lease, &j.Attempts)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	var embedding string
	var text string
	var deleted bool
	var version int
	if j.Kind == "post" {
		e = w.Store.Pool.QueryRow(ctx, "select text,version,deleted_at is not null or moderation_status<>'allowed'from feed.posts where id=$1", j.Target).Scan(&text, &version, &deleted)
		if errors.Is(e, pgx.ErrNoRows) {
			deleted = true
			e = nil
		}
		if e == nil && !deleted && version == j.Version && text != "" {
			j.EmbeddingPost = j.Target
			embedding, e = w.Model.Embed(ctx, text)
		}
	} else {
		// Interactive feedback takes priority over a publication backlog. A missing
		// vector is computed outside the transaction, then committed with the event.
		e = w.Store.Pool.QueryRow(ctx, `select p.id::text,p.text,p.version from feed.events ev join feed.posts p on p.id=ev.post_id where ev.id=$1 and ev.kind not in('impression','open_post')and ev.post_version=p.version and p.deleted_at is null and p.moderation_status='allowed'and p.text<>''and not exists(select 1 from recsys.post_embeddings em where em.post_id=p.id and em.post_version=p.version and em.model=$2)`, j.Target, w.Store.Model).Scan(&j.EmbeddingPost, &text, &version)
		if errors.Is(e, pgx.ErrNoRows) {
			e = nil
		} else if e == nil {
			embedding, e = w.Model.Embed(ctx, text)
		}
	}
	if e == nil {
		e = w.apply(ctx, j, embedding, deleted, version)
	}
	if e != nil {
		delay := time.Duration(math.Min(300, math.Pow(2, float64(j.Attempts)))) * time.Second
		_, retryErr := w.Store.Pool.Exec(ctx, "update recsys.jobs set last_error=$3,available_at=now()+$4::interval,locked_until=null,lease=null where id=$1 and lease=$2", j.ID, j.Lease, "processing_failed", fmt.Sprintf("%f seconds", delay.Seconds()))
		if retryErr != nil {
			return true, retryErr
		}
	}
	return true, e
}
func (w *Worker) apply(ctx context.Context, j job, embedding string, deleted bool, version int) error {
	tx, e := w.Store.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	var owned bool
	if e = tx.QueryRow(ctx, "select true from recsys.jobs where id=$1 and lease=$2 and done_at is null for update", j.ID, j.Lease).Scan(&owned); errors.Is(e, pgx.ErrNoRows) {
		return nil
	} else if e != nil {
		return e
	}
	if embedding != "" && j.EmbeddingPost != "" {
		_, e = tx.Exec(ctx, `insert into recsys.post_embeddings(post_id,post_version,model,embedding)select id,version,$3,$4::extensions.vector from feed.posts where id=$1 and version=$2 and deleted_at is null and moderation_status='allowed'on conflict(post_id)do update set post_version=excluded.post_version,model=excluded.model,embedding=excluded.embedding,updated_at=now()`, j.EmbeddingPost, version, w.Store.Model, embedding)
		if e != nil {
			return e
		}
	}
	if j.Kind == "post" {
		if deleted {
			_, e = tx.Exec(ctx, "delete from recsys.post_embeddings where post_id=$1", j.Target)
		}
	} else {
		e = w.event(ctx, tx, j.Target)
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "update recsys.jobs set done_at=now(),locked_until=null,last_error=null where id=$1 and lease=$2", j.ID, j.Lease); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (w *Worker) event(ctx context.Context, tx pgx.Tx, id string) error {
	var user, post, kind, text string
	var active, eventVersion, currentVersion int
	var at time.Time
	e := tx.QueryRow(ctx, `select e.user_id::text,e.post_id::text,e.kind,e.active_ms,e.created_at,p.text,e.post_version,p.version from feed.events e join feed.posts p on p.id=e.post_id where e.id=$1`, id).Scan(&user, &post, &kind, &active, &at, &text, &eventVersion, &currentVersion)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "select pg_advisory_xact_lock(hashtextextended($1,0))", user); e != nil {
		return e
	}
	tag, e := tx.Exec(ctx, "insert into recsys.applied_events values($1)on conflict do nothing", id)
	if e != nil || tag.RowsAffected() == 0 {
		return e
	}
	impression := 0
	if kind == "impression" {
		impression = 1
		_, e = tx.Exec(ctx, "insert into recsys.seen(user_id,post_id,seen_at)values($1,$2,$3)on conflict(user_id,post_id)do update set seen_at=greatest(recsys.seen.seen_at,excluded.seen_at)", user, post, at)
		if e != nil {
			return e
		}
	}
	_, e = tx.Exec(ctx, `insert into recsys.post_stats(post_id,impressions,likes,reposts)values($1,$2,(select count(*)from feed.likes where post_id=$1),(select count(*)from feed.reposts where post_id=$1))on conflict(post_id)do update set impressions=recsys.post_stats.impressions+excluded.impressions,likes=excluded.likes,reposts=excluded.reposts`, post, impression)
	if e != nil {
		return e
	}
	var allowed bool
	if e = tx.QueryRow(ctx, "select recsys.account_allowed($1)", user).Scan(&allowed); e != nil {
		return e
	}
	if !allowed {
		return nil
	}
	var currentEpoch bool
	if e = tx.QueryRow(ctx, "select $2::timestamptz>=coalesce((select reset_at from recsys.user_state where user_id=$1),'-infinity'::timestamptz)", user, at).Scan(&currentEpoch); e != nil {
		return e
	}
	if !currentEpoch || eventVersion != currentVersion {
		return nil
	}
	weight, canonical := 0.0, kind
	switch kind {
	case "like":
		weight = 2
	case "unlike":
		canonical = "like"
	case "repost":
		weight = 3
	case "unrepost":
		canonical = "repost"
	case "comment":
		weight = 0.5
	case "comment_like":
		weight = 0.3
	case "open_photo":
		weight = 0.3
	case "dwell":
		weight = math.Min(2, float64(active)/math.Max(2000, float64(len([]rune(text)))*45))
	case "not_interested":
		weight = -3
	case "report":
		// A complaint about a comment is not disinterest in the parent post.
		var postReport bool
		if e = tx.QueryRow(ctx, "select exists(select 1 from feed.reports where reporter_id=$1 and post_id=$2)", user, post).Scan(&postReport); e != nil {
			return e
		}
		if !postReport {
			return nil
		}
		weight = -3
	default:
		return nil
	}
	// Last-write wins by server event time: delayed jobs cannot resurrect an unlike.
	_, e = tx.Exec(ctx, `insert into recsys.evidence(user_id,post_id,kind,weight,updated_at,post_version)values($1,$2,$3,$4,$5,$6)on conflict(user_id,post_id,kind)do update set weight=excluded.weight,updated_at=excluded.updated_at,post_version=excluded.post_version where recsys.evidence.updated_at<=excluded.updated_at`, user, post, canonical, weight, at, eventVersion)
	if e != nil {
		return e
	}
	return w.rebuild(ctx, tx, user)
}

type cluster struct {
	vector   []float64
	mass     float64
	negative bool
}

func (w *Worker) rebuild(ctx context.Context, tx pgx.Tx, user string) error {
	rows, e := tx.Query(ctx, `select em.embedding::text,ev.weight,ev.updated_at from recsys.evidence ev join recsys.post_embeddings em on em.post_id=ev.post_id and em.model=$2 and em.post_version=ev.post_version join feed.posts p on p.id=ev.post_id and p.version=em.post_version left join recsys.user_state us on us.user_id=ev.user_id where ev.user_id=$1 and ev.weight<>0 and ev.updated_at>=coalesce(us.reset_at,'-infinity'::timestamptz)and ev.updated_at>now()-interval '30 days'and p.deleted_at is null and p.moderation_status='allowed'and recsys.can_view($1,p.author_id,p.audience)order by ev.updated_at desc,ev.post_id,ev.kind limit 200`, user, w.Store.Model)
	if e != nil {
		return e
	}
	clusters := []cluster{}
	now := time.Now().UTC()
	for rows.Next() {
		var raw string
		var weight float64
		var at time.Time
		if e = rows.Scan(&raw, &weight, &at); e != nil {
			rows.Close()
			return e
		}
		v := domain.ParseVector(raw)
		negative := weight < 0
		mass := math.Abs(weight) * math.Exp(-math.Max(0, now.Sub(at).Hours())/(24*7))
		best := -1
		similar := -2.0
		count := 0
		for i, c := range clusters {
			if c.negative != negative {
				continue
			}
			count++
			s := domain.Cosine(v, c.vector)
			if s > similar {
				best, similar = i, s
			}
		}
		if best < 0 || (similar < 0.82 && count < 4) {
			clusters = append(clusters, cluster{v, mass, negative})
		} else {
			c := &clusters[best]
			for i := range v {
				c.vector[i] = (c.vector[i]*c.mass + v[i]*mass) / (c.mass + mass)
			}
			c.mass += mass
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "delete from recsys.interests where user_id=$1", user); e != nil {
		return e
	}
	for i, c := range clusters {
		v, e := domain.Vector(c.vector)
		if e != nil {
			continue
		}
		if _, e = tx.Exec(ctx, "insert into recsys.interests values($1,$2,$3,$4,$5::extensions.vector,$6,$7)", user, i, c.negative, w.Store.Model, v, c.mass, now); e != nil {
			return e
		}
	}
	return nil
}

func (w *Worker) Cleanup(ctx context.Context) error {
	// Retention runs as bounded batches, separate from user requests.
	_, e := w.Store.Pool.Exec(ctx, "delete from recsys.sessions where id in(select id from recsys.sessions where expires_at<now()-interval '1 day'limit 1000)")
	if e != nil {
		return e
	}
	_, e = w.Store.Pool.Exec(ctx, "delete from recsys.jobs where id in(select id from recsys.jobs where done_at<now()-interval '7 days'limit 5000)")
	return e
}
