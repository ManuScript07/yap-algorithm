package store

import (
	"context"
	"github.com/jackc/pgx/v5"
	"strings"
	"yap-algorithm/internal/domain"
)

type Comment struct {
	ID       string  `json:"id"`
	ParentID *string `json:"parent_id,omitempty"`
	Text     string  `json:"text"`
}

func (s *Store) Comment(ctx context.Context, user, post string, in Comment) (Comment, error) {
	if !domain.UUIDValid(in.ID) || strings.TrimSpace(in.Text) == "" || len([]rune(in.Text)) > 5000 || (in.ParentID != nil && !domain.UUIDValid(*in.ParentID)) {
		return in, domain.ErrInvalid
	}
	e := s.WithUser(ctx, user, func(tx pgx.Tx) error {
		if in.ParentID != nil {
			var ok bool
			if e := tx.QueryRow(ctx, "select exists(select 1 from feed.comments where id=$1 and post_id=$2)", *in.ParentID, post).Scan(&ok); e != nil {
				return e
			}
			if !ok {
				return domain.ErrNotFound
			}
		}
		tag, e := tx.Exec(ctx, "insert into feed.comments(id,post_id,author_id,parent_id,text)values($1,$2,$3,$4,$5)on conflict(id)do nothing", in.ID, post, user, in.ParentID, in.Text)
		if e != nil {
			return e
		}
		if tag.RowsAffected() == 0 {
			var owner, pid, text string
			var parent *string
			if e = tx.QueryRow(ctx, "select author_id::text,post_id::text,text,parent_id::text from feed.comments where id=$1", in.ID).Scan(&owner, &pid, &text, &parent); e != nil {
				return domain.ErrInvalidated
			}
			sameParent := (parent == nil && in.ParentID == nil) || (parent != nil && in.ParentID != nil && *parent == *in.ParentID)
			if owner != user || pid != post || text != in.Text || !sameParent {
				return domain.ErrInvalidated
			}
		}
		return nil
	})
	return in, e
}
func (s *Store) CommentLike(ctx context.Context, user, comment string, like bool) error {
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		if like {
			_, e := tx.Exec(ctx, "insert into feed.comment_likes values($1,$2)on conflict do nothing", user, comment)
			return e
		}
		_, e := tx.Exec(ctx, "delete from feed.comment_likes where user_id=$1 and comment_id=$2", user, comment)
		return e
	})
}
func (s *Store) Report(ctx context.Context, user, target, reason string, comment bool) error {
	switch reason {
	case "spam", "harassment", "violence", "sexual", "other":
	default:
		return domain.ErrInvalid
	}
	return s.WithUser(ctx, user, func(tx pgx.Tx) error {
		column := "post_id"
		if comment {
			column = "comment_id"
		}
		_, e := tx.Exec(ctx, "insert into feed.reports(id,reporter_id,"+column+",reason)values($1,$2,$3,$4)on conflict do nothing", domain.UUID(), user, target, reason)
		return e
	})
}
