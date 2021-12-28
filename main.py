import psycopg2
from psycopg2 import Error
import matplotlib.pyplot as plt


def get_connection():
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="Lab_2",
        user="postgres",
        password="23032002")
    return conn


def create_views(cursor):
    cursor.execute("""
        CREATE VIEW WINNERS_COUNT AS
           SELECT winner, count(*)
           FROM public.games
           GROUP BY winner;
    """)
    cursor.execute("""
        CREATE VIEW RATED_COUNT AS
           SELECT rated, count(*)
           FROM public.games
           GROUP BY rated;
    """)
    cursor.execute("""
        CREATE VIEW STATUSES_COUNT AS
            SELECT game_status, count(*)
            FROM public.games
            GROUP BY game_status;
    """)


def drop_views(cursor):
    cursor.execute("""
        DROP VIEW WINNERS_COUNT
    """)
    cursor.execute("""
        DROP VIEW RATED_COUNT
    """)
    cursor.execute("""
        DROP VIEW STATUSES_COUNT
    """)


def statistics(conn):
    cursor = conn.cursor()
    res = []
    try:
        create_views(cursor)
        cursor.execute("""
            SELECT * FROM WINNERS_COUNT
        """)

        res.append(cursor.fetchall())

        cursor.execute("""
            SELECT * FROM RATED_COUNT
        """)
        res.append(cursor.fetchall())

        cursor.execute("""
            SELECT * FROM STATUSES_COUNT
        """)
        res.append(cursor.fetchall())
        drop_views(cursor)
    except (Exception, Error) as e:
        print(e)
    return res


def draw(res):
    winners = res[0]
    types = [el[0] for el in winners]
    count_of_win = [el[1] for el in winners]
    fig = plt.figure(figsize=(5, 5))
    plt.bar(types, count_of_win, color='green',
            width=0.4)

    plt.xlabel("Count")
    plt.ylabel("Type")
    plt.title("Counts of winner types")
    plt.show()

    statuses = res[2]
    status = [el[0] for el in statuses]
    count_of_state = [el[1] for el in statuses]
    fig = plt.figure(figsize=(5, 5))
    plt.bar(status, count_of_state, color='green',
            width=0.4)

    plt.xlabel("Count")
    plt.ylabel("Status")
    plt.title("Counts of game statuses types")
    plt.show()

    rates = res[1]
    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = [str(el[0]) for el in rates]
    sizes = [el[1] / 10 for el in rates]

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title("Rated?")
    plt.show()


if __name__ == '__main__':
    conn = get_connection()
    res = statistics(conn)
    draw(res)
