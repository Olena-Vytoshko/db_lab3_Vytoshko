from datetime import datetime
import psycopg2
import csv


def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="Lab_2",
        user="postgres",
        password="23032002")
    return conn


def insert_games_into_table(cur, csv_file):
    def get_values(row):
        val = {
            'game_id': row['id'],
            'white_player_id': row['white_id'],
            'white_player_rating': row['white_rating'],
            'black_player_rating': row['black_rating'],
            'black_player_id': row['black_id'],
            'rated': row['rated'],
            'start_time': row['created_at'],
            'end_time': row['last_move_at'],
            'number_of_turns': row['turns'],
            'game_status': row['victory_status'],
            'winner': row['winner']}

        return val
    i = 1
    cur = conn.cursor()
    for row in csv_file:
        values = get_values(row)
        values['white_player_registration_date'] = '1/1/2008'
        values['black_player_registration_date'] = '31/12/2010'
        cur.execute(f"""
        INSERT INTO public.white_player (white_player_id, white_player_registration_date, white_player_rating)
        VALUES('{values['white_player_id'] + str(i)}', 
                '{values['white_player_registration_date']}',
                '{values['white_player_rating']}')
        """)

        cur.execute(f"""
                INSERT INTO public.black_player (black_player_id, black_player_registration_date, black_player_rating)
                VALUES('{values['black_player_id'] + str(i)}', 
                        '{values['black_player_registration_date']}',
                        '{values['black_player_rating']}')
        """)
        cur.execute(f"""
                        INSERT INTO public.games(
                        game_id,
                        white_player_id,
                        black_player_id,
                        rated,
                        start_time,
                        end_time,
                        number_of_turns,
                        game_status,
                        winner)
                        VALUES(
                        '{values['game_id'] + str(i)}',
                        '{values['white_player_id'] + str(i)}',
                        '{values['black_player_id'] + str(i)}',
                        '{values['rated']}',
                        '{datetime.fromtimestamp(int(float(values['start_time'])) / 1e3).strftime("%Y-%m-%d, %H:%M:%S")}',
                        '{datetime.fromtimestamp(int(float(values['end_time'])) / 1e3).strftime("%Y-%m-%d, %H:%M:%S")}',
                        '{int(values['number_of_turns'])}',
                        '{values['game_status']}',
                        '{values['winner']}')
        """)
        i += 1
    cur.close()


if __name__ == '__main__':
    conn = get_connection()
    input_file = csv.DictReader(open('C:\\Users\\38096\\Desktop\\lab3\\games.csv', 'r'))
    insert_games_into_table(conn, input_file)
    conn.commit()
    conn.close()