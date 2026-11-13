package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"yap-algorithm/internal/domain"
)

func createLoadChats(t *testing.T, pool *pgxpool.Pool, readers []string, peer string) []string {
	t.Helper()
	chats := make([]string, len(readers))
	for i := range chats {
		chats[i] = domain.UUID()
	}
	t.Cleanup(func() { sbExec(t, pool, "delete from public.conversations where id=any($1::uuid[])", chats) })
	sbExec(t, pool, `insert into public.conversations(id,user_one_id,user_two_id,created_by)
		select chat,least(reader,$3::uuid),greatest(reader,$3::uuid),reader from unnest($1::uuid[],$2::uuid[]) f(chat,reader)`, chats, readers, peer)
	sbExec(t, pool, `insert into public.conversation_members(conversation_id,user_id)
		select c.id,u.id from public.conversations c cross join lateral (values(c.user_one_id),(c.user_two_id)) u(id) where c.id=any($1::uuid[])`, chats)
	sbExec(t, pool, `insert into public.messages(id,conversation_id,sender_id,type,text)
		select extensions.gen_random_uuid(),chat,$2,'text','Synthetic message' from unnest($1::uuid[]) chat`, chats, peer)
	return chats
}

var mixedFeedRPCs = []string{"get_chat_summaries_by_ids", "get_feed_comments", "apply_feed_action"}

type mixedActionState struct {
	sync.Mutex
	receipt string
	version int64
	liked   bool
}

type mixedFeedProbes struct {
	client                soakHTTP
	tokens, chats         []string
	anchorPost, commentID string
	actions               []mixedActionState
}

func newMixedFeedProbes(t *testing.T, p *pgxpool.Pool, client soakHTTP, readers, tokens []string, author string) *mixedFeedProbes {
	t.Helper()
	probe := &mixedFeedProbes{client: client, tokens: tokens, chats: createLoadChats(t, p, readers, author),
		actions: make([]mixedActionState, len(readers)), commentID: domain.UUID()}
	if err := p.QueryRow(context.Background(), "select id::text from feed.posts where author_id=$1 order by created_at desc,id limit 1", author).Scan(&probe.anchorPost); err != nil {
		t.Fatal(err)
	}
	stage5RPC(t, readers[0], "create_feed_comment", map[string]any{"requested_id": probe.commentID,
		"requested_post": probe.anchorPost, "requested_text": "Synthetic mixed load comment"}, 200)
	for i, reader := range readers {
		page := readScroll(t, reader, domain.UUID(), "", 1)
		if len(page.Items) != 1 {
			t.Fatal("engagement probe requires a delivered post")
		}
		item := page.Items[0]
		probe.actions[i].receipt, probe.actions[i].version, probe.actions[i].liked = item.Receipt, item.Post.ActionVersion, item.Post.Liked
	}
	return probe
}

func (probe *mixedFeedProbes) request(index int, due time.Time) (feedPerfSample, string) {
	reader := index % len(probe.tokens)
	name := mixedFeedRPCs[index/len(probe.tokens)%len(mixedFeedRPCs)]
	var args map[string]any
	var want string
	var action *mixedActionState
	switch name {
	case "get_chat_summaries_by_ids":
		args = map[string]any{"conversation_ids": []string{probe.chats[reader]}}
		want = probe.chats[reader]
	case "get_feed_comments":
		args = map[string]any{"requested_post": probe.anchorPost, "requested_limit": 20}
		want = probe.commentID
	case "apply_feed_action":
		action = &probe.actions[reader]
		action.Lock()
		defer action.Unlock()
		kind := "like"
		if action.liked {
			kind = "unlike"
		}
		args = map[string]any{"requested_id": domain.UUID(), "requested_delivery": action.receipt,
			"requested_action": kind, "expected_version": action.version}
	}
	start := time.Now()
	body, status, err := probe.client.rpc(probe.tokens[reader], name, args)
	sample := feedPerfSample{RPC: name, Page: "mixed", HTTPMS: float64(time.Since(start)) / float64(time.Millisecond),
		AdmissionMS: float64(start.Sub(due)) / float64(time.Millisecond), Status: status, TransportError: err != nil}
	var netError net.Error
	sample.Timeout = errors.As(err, &netError) && netError.Timeout()
	if err != nil || status != 200 {
		var detail struct{ Code string }
		_ = json.Unmarshal(body, &detail)
		sample.ErrorCode = detail.Code
		return sample, fmt.Sprintf("%s status=%d transport=%t code=%s", name, status, err != nil, detail.Code)
	}
	if action == nil {
		if !bytes.Contains(body, []byte(want)) {
			return sample, name + " did not return the fixture"
		}
		return sample, ""
	}
	var result struct {
		Version int64 `json:"action_version"`
		Liked   bool  `json:"liked"`
	}
	if json.Unmarshal(body, &result) != nil || result.Version != action.version+1 || result.Liked == action.liked {
		return sample, "mixed action did not advance the expected state"
	}
	action.version, action.liked = result.Version, result.Liked
	return sample, ""
}
