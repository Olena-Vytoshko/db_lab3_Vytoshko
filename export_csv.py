import psycopg2
from psycopg2 import Error
import os

def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="Lab_2",
        user="postgres",
        password="23032002")
    return conn


def export_csv(conn, output_folder_path):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT * FROM public.games
        """)
        games = cursor.fetchall()
        games_schema = ['game_id', 'white_player_id', 'black_player_id', 'rated', 'start_time', 'end_time',
                        'number_of_turns', 'game_status', 'winner']

        write_in_csv(
            os.path.join(output_folder_path, 'games_table.csv'),
            games,
            games_schema
        )

        cursor.execute("""
                   SELECT * FROM public.white_player
               """)
        white_player = cursor.fetchall()
        white_player_schema = ['white_player_id', 'white_player_registration_date', 'white_player_rating']

        write_in_csv(
            os.path.join(output_folder_path, 'white_player_table.csv'),
            white_player,
            white_player_schema
        )

        cursor.execute("""
                           SELECT * FROM public.white_player
                       """)
        black_player = cursor.fetchall()
        black_player_schema = ['black_player_id', 'black_player_registration_date', 'black_player_rating']

        write_in_csv(
            os.path.join(output_folder_path, 'black_player_table.csv'),
            black_player,
            black_player_schema
        )

    except (Exception, Error) as e:
        print(e)


def write_in_csv(file_name, data, schema):
    with open(file_name, 'w') as out:
        out.write(','.join(schema) + '\n')
        for row in data:
            str_row = (str(el) for el in row)
            out.write(','.join(str_row) + '\n')


if __name__ == '__main__':
    conn = get_connection()
    export_csv(conn, 'C:\\Users\\38096\\Desktop\\lab3')
    conn.close()
