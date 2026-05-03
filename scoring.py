import json
import logging
import db

POINT_SYSTEM = {
    'run': 1,
    'four': 4,
    'six': 6,
    'wicket': 25,
    'maiden': 10 # Assuming maiden over points
}
CAPTAIN_MULTIPLIER = 2.0
VC_MULTIPLIER = 1.5

def update_player_stat_only(match_id, player_name, event_type):
    """Sirf player ke individual stats update karta hai (UI Refresh ke liye fast)"""
    player_name = player_name.replace('_', ' ').strip()
    points = POINT_SYSTEM.get(event_type, 0)
    
    with db.get_db() as conn:
        col = "runs" if event_type == 'run' else event_type + "s" if event_type in ['four', 'six'] else "wickets"
        conn.execute(f"""
            INSERT INTO PLAYER_LIVE_STATS (match_id, player_name, {col}) 
            VALUES (%s, %s, 1)
            ON CONFLICT(match_id, player_name) DO UPDATE SET {col} = PLAYER_LIVE_STATS.{col} + 1
        """, (match_id, player_name))

        # 2. Log Event
        conn.execute("INSERT INTO MATCH_EVENTS (match_id, player_name, event_type, points_awarded) VALUES (%s,%s,%s,%s)",
                     (match_id, player_name, event_type, points))
    return True

def update_team_points_incrementally(match_id, player_name, event_type):
    """Hazaaron teams ke points background mein update karta hai"""
    player_name = player_name.replace('_', ' ').strip()
    points = POINT_SYSTEM.get(event_type, 0)
    
    with db.get_db() as conn:
        p_like = f"%{player_name}%"

        # normal players
        conn.execute("""
            UPDATE TEAMS SET points = points + %s 
            WHERE match_id = %s AND team_players LIKE %s 
            AND (captain IS NULL OR captain NOT LIKE %s) 
            AND (vice_captain IS NULL OR vice_captain NOT LIKE %s)
        """, (points, match_id, p_like, p_like, p_like))
        
        # Captains (2x)
        conn.execute("UPDATE TEAMS SET points = points + %s WHERE match_id = %s AND captain LIKE %s", (points * CAPTAIN_MULTIPLIER, match_id, p_like))
        
        # VCs (1.5x)
        conn.execute("UPDATE TEAMS SET points = points + %s WHERE match_id = %s AND vice_captain LIKE %s", (points * VC_MULTIPLIER, match_id, p_like))

def update_match_event(match_id, player_name, event_type):
    """Legacy wrapper for backward compatibility"""
    update_player_stat_only(match_id, player_name, event_type)
    update_team_points_incrementally(match_id, player_name, event_type)
    return True

def recalculate_match_points(match_id):
    """
    Nuclear Option: Purani saari galtiyon ko theek karta hai. 
    PLAYER_LIVE_STATS se fresh points uthakar har team ka score zero se calculate karta hai.
    """
    try:
        stats_map = db.db_get_player_live_stats_map(match_id)
        
        def get_p_pts(name):
            s = stats_map.get(name, {'runs': 0, 'fours': 0, 'sixes': 0, 'wickets': 0})
            return (s['runs'] * POINT_SYSTEM['run'] + 
                    s['fours'] * POINT_SYSTEM['four'] + 
                    s['sixes'] * POINT_SYSTEM['six'] + 
                    s['wickets'] * POINT_SYSTEM['wicket'])

        with db.get_db() as conn:
            conn.execute("SELECT user_id, team_num, team_players, captain, vice_captain FROM TEAMS WHERE match_id=%s AND is_paid=1", (match_id,))
            teams = conn.fetchall()
            
            for t in teams:
                players_data = json.loads(t['team_players'])
                t_total = 0
                for role in players_data:
                    for p_full in players_data[role]:
                        p_name = str(p_full).split(' (')[0].strip()
                        p_pts = get_p_pts(p_name)
                        
                        # Multiplier check
                        mult = 1.0
                        if p_full == t['captain']: mult = CAPTAIN_MULTIPLIER
                        elif p_full == t['vice_captain']: mult = VC_MULTIPLIER
                        
                        t_total += (p_pts * mult)
                
                conn.execute("UPDATE TEAMS SET points = %s WHERE user_id=%s AND match_id=%s AND team_num=%s", 
                             (t_total, t['user_id'], match_id, t['team_num']))
        return True
    except Exception as e:
        logging.error(f"Recalculate error: {e}")
        return False
