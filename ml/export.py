"""Legacy fixture-only proxy export, not the production feedback-v1 dataset.

The historical max/any proxy cannot model cancellations or comment report targets.
Keep it isolated for prototype comparisons; current facts use ml/feedback.py and
require the separate point-in-time dataset pipeline before model selection.
"""
import argparse
import json
from pathlib import Path
import subprocess


def query(sql):
    p = subprocess.run(["docker", "exec", "-i", "yap_algorithm_db", "psql", "-U", "supabase_admin", "-d", "yap_algorithm", "-qAt", "-v", "ON_ERROR_STOP=1"], input=sql, text=True, capture_output=True, encoding="utf-8", check=True)
    return p.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("output/dataset.jsonl"))
    parser.add_argument("--window-minutes", type=int, default=30)
    parser.add_argument("--synthetic-immediate", action="store_true")
    args = parser.parse_args()
    if query("select current_database()='yap_algorithm'and recsys.is_fixture();").strip() != "t":
        raise ValueError("Local fixture export only")
    if not 1 <= args.window_minutes <= 10080:
        raise ValueError("window_minutes")
    maturity = "true" if args.synthetic_immediate else f"ev.created_at<now()-interval '{args.window_minutes} minutes'"
    # Only actual confirmed impressions; the features were captured before actions.
    # Label 0 = no positive signal in the window, not a declared personal dislike.
    source = f"""
    select jsonb_build_object('group',s.id,'at',s.created_at,'user_id',s.user_id,'post_id',i.post_id,
      'features',i.features,'baseline',i.score,'origin','synthetic',
      'embedding_model','intfloat/multilingual-e5-small@614241f622f53c4eeff9890bdc4f31cfecc418b3:onnx-int8:query',
      'label',case when coalesce(f.negative,false)then 0 else coalesce(f.positive,0)end)
    from recsys.sessions s join recsys.session_items i on i.session_id=s.id
    join feed.events ev on ev.session_id=s.id and ev.position=i.position and ev.kind='impression'
    left join lateral(select bool_or(e.kind in('not_interested','report'))negative,
      max(case e.kind when 'repost'then 4 when 'like'then 3 when 'dwell'then 2 when 'comment'then 1 else 0 end)positive
      from feed.events e where e.user_id=s.user_id and e.post_id=i.post_id
      and e.created_at>=ev.created_at and e.created_at<=ev.created_at+interval '{args.window_minutes} minutes')f on true
    where s.tab='for_you'and cardinality(i.features)=8 and {maturity}
    order by s.created_at,s.id,i.position;
    """
    lines = query(source).splitlines()
    rows = [json.loads(line) for line in lines if line.strip()]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    print(json.dumps({"rows": len(rows), "groups": len({r['group'] for r in rows}), "origin": "synthetic", "matured": not args.synthetic_immediate}))


if __name__ == "__main__":
    main()
