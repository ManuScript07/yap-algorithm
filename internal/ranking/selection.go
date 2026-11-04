package ranking

// SelectForYou prefers unseen posts for 24h. Previously seen posts are a
// deliberate small-inventory fallback, never a way to fill a large cache at the
// cost of showing read posts while enough fresh candidates exist.
func SelectForYou(candidates []Candidate) []Candidate {
	unseen := make([]Candidate, 0, len(candidates))
	seen := make([]Candidate, 0)
	ids := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		if c.ID == "" || ids[c.ID] {
			continue
		}
		ids[c.ID] = true
		if c.Seen {
			seen = append(seen, c)
		} else {
			unseen = append(unseen, c)
		}
	}
	out := Rerank(unseen, 200, 10)
	if len(unseen) < 20 {
		out = append(out, Rerank(seen, 200-len(out), 10)...)
	}
	return out
}
