import json
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

def update_match_event(match_id, player_name, event_type):
    player_name = player_name.replace('_', ' ') # Fix: Pat_Cummins -> Pat Cummins
    points = POINT_SYSTEM.get(event_type, 0)
    
    with db.get_db() as conn:
        # 1. Update Player Stats
        col = "runs" if event_type == 'run' else event_type + "s" if event_type in ['four', 'six'] else "wickets"
        conn.execute(f"""
            INSERT INTO PLAYER_LIVE_STATS (match_id, player_name, {col}) 
            VALUES (%s, %s, 1)
            ON CONFLICT(match_id, player_name) DO UPDATE SET {col} = PLAYER_LIVE_STATS.{col} + 1
        """, (match_id, player_name))

        # 2. Log Event
        conn.execute("INSERT INTO MATCH_EVENTS (match_id, player_name, event_type, points_awarded) VALUES (%s,%s,%s,%s)",
                     (match_id, player_name, event_type, points))

        # 3. ⚡ BULK UPDATE TEAMS (Zero Lag Logic)
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

    return True
