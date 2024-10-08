import http.client
import json
import sqlite3
import pandas as pd
import os

BASE_URL = "imdb188.p.rapidapi.com"
API_KEY = "6abccfabd4msh4b1adf3f2137a45p1db720jsne24fe50c0600"

# function to fetch popular celebrities
def fetch_popular_celebrities():
    conn = http.client.HTTPSConnection(BASE_URL)
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': BASE_URL
    }
    
    conn.request("GET", "/api/v1/getPopularCelebrities", headers=headers)
    res = conn.getresponse()
    
    print(f"Response Status: {res.status}")  # Log the response status
    if res.status != 200:
        print(f"Failed to fetch celebrities: {res.status} {res.reason}")
        return []
    
    data = json.loads(res.read().decode("utf-8"))
    return data.get('celebrities', [])

# function to fetch popular movies
def fetch_popular_movies():
    conn = http.client.HTTPSConnection(BASE_URL)
    payload = json.dumps({
        "country": {"anyPrimaryCountries": ["IN"]},
        "limit": 200,
        "releaseDate": {"releaseDateRange": {"end": "2029-12-31", "start": "2020-01-01"}},
        "userRatings": {"aggregateRatingRange": {"max": 10, "min": 6}, "ratingsCountRange": {"min": 1000}},
        "genre": {"allGenreIds": ["Action"]},
        "runtime": {"runtimeRangeMinutes": {"max": 120, "min": 0}}
    })
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': BASE_URL,
        'Content-Type': "application/json"
    }

    conn.request("POST", "/api/v1/getPopularMovies", payload, headers)
    res = conn.getresponse()
    
    print(f"Response Status: {res.status}")  # Log the response status
    if res.status != 200:
        print(f"Failed to fetch movies: {res.status} {res.reason}")
        return []
    
    data = json.loads(res.read().decode("utf-8"))
    return data.get('results', [])

# function to fetch popular TV shows
def fetch_popular_tv_shows():
    conn = http.client.HTTPSConnection(BASE_URL)
    payload = json.dumps({
        "country": {"anyPrimaryCountries": ["IN"]},
        "limit": 200,
        "userRatings": {"aggregateRatingRange": {"max": 10, "min": 6}, "ratingsCountRange": {"min": 1000}}
    })
    headers = {
        'x-rapidapi-key': API_KEY,
        'x-rapidapi-host': BASE_URL,
        'Content-Type': "application/json"
    }

    conn.request("POST", "/api/v1/getPopularTVShows", payload, headers)
    res = conn.getresponse()
    
    print(f"Response Status: {res.status}")  # Log the response status
    if res.status != 200:
        print(f"Failed to fetch TV shows: {res.status} {res.reason}")
        return []
    
    data = json.loads(res.read().decode("utf-8"))
    return data.get('results', [])

# function to create SQLite tables with relationships and insert data
def create_and_insert_data(celebrities, movies, tv_shows):
    conn = sqlite3.connect('imdb_data.db')
    cursor = conn.cursor()
    
    # enable foreign key support
    cursor.execute('PRAGMA foreign_keys = ON')
    
    # create tables with foreign key relationships
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Celebrities (
        id INTEGER PRIMARY KEY,
        name TEXT,
        birth_date TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Movies (
        id INTEGER PRIMARY KEY,
        title TEXT,
        release_date TEXT,
        rating REAL,
        celebrity_id INTEGER,
        FOREIGN KEY (celebrity_id) REFERENCES Celebrities(id) ON DELETE SET NULL
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS TVShows (
        id INTEGER PRIMARY KEY,
        title TEXT,
        release_date TEXT,
        rating REAL,
        celebrity_id INTEGER,
        FOREIGN KEY (celebrity_id) REFERENCES Celebrities(id) ON DELETE SET NULL
    )
    ''')

    # insert celebrities
    for celeb in celebrities:
        cursor.execute('''
        INSERT INTO Celebrities (id, name, birth_date) VALUES (?, ?, ?)
        ''', (celeb.get('id'), celeb.get('name'), celeb.get('birth_date')))
    
    # insert movies (assuming a celebrity_id field)
    for movie in movies:
        cursor.execute('''
        INSERT INTO Movies (id, title, release_date, rating, celebrity_id) VALUES (?, ?, ?, ?, ?)
        ''', (movie.get('id'), movie.get('title'), movie.get('releaseDate'), movie.get('userRating'), None))  # None for celebrity_id

    # insert TV shows (assuming a celebrity_id field)
    for show in tv_shows:
        cursor.execute('''
        INSERT INTO TVShows (id, title, release_date, rating, celebrity_id) VALUES (?, ?, ?, ?, ?)
        ''', (show.get('id'), show.get('title'), show.get('releaseDate'), show.get('userRating'), None))  # None for celebrity_id

    # commit and close
    conn.commit()
    conn.close()
    print("Data inserted into database successfully.")

# function to generate a SQL report
def generate_sql_report():
    conn = sqlite3.connect('imdb_data.db')
    cursor = conn.cursor()
    
    # Example report: Top-rated movies and TV shows with their ratings
    cursor.execute('''
    SELECT title, rating, 'Movie' AS type FROM Movies
    UNION ALL
    SELECT title, rating, 'TV Show' AS type FROM TVShows
    ORDER BY rating DESC
    LIMIT 10
    ''')
    
    results = cursor.fetchall()
    print("\nTop Rated Movies and TV Shows:")
    for row in results:
        print(f"Title: {row[0]}, Rating: {row[1]}, Type: {row[2]}")
    
    conn.close()

# function to export data to Excel
def export_to_excel():
    conn = sqlite3.connect('imdb_data.db')

    # coad data into DataFrames
    celebrities_df = pd.read_sql_query("SELECT * FROM Celebrities", conn)
    movies_df = pd.read_sql_query("SELECT * FROM Movies", conn)
    tv_shows_df = pd.read_sql_query("SELECT * FROM TVShows", conn)

    # check if DataFrames are empty before exporting
    if celebrities_df.empty and movies_df.empty and tv_shows_df.empty:
        print("No data to export.")
        return
    
    # convert birth_date to datetime
    if 'birth_date' in celebrities_df.columns:
        celebrities_df['birth_date'] = pd.to_datetime(celebrities_df['birth_date'], errors='coerce')

    # file path for the Excel file
    file_path = 'imdb_info.xlsx'
    
    # check if file exists and delete it if so
    if os.path.exists(file_path):
        os.remove(file_path)

    # Export to Excel
    with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
        celebrities_df.to_excel(writer, sheet_name='Celebrities', index=False)
        movies_df.to_excel(writer, sheet_name='Movies', index=False)
        tv_shows_df.to_excel(writer, sheet_name='TV Shows', index=False)

    conn.close()
    print("Data exported to Excel successfully.")

# main function to execute the script
def main():
    celebrities = fetch_popular_celebrities()
    print(f"Fetched {len(celebrities)} celebrities.")
    movies = fetch_popular_movies()
    print(f"Fetched {len(movies)} movies.")
    tv_shows = fetch_popular_tv_shows()
    print(f"Fetched {len(tv_shows)} TV shows.")

    create_and_insert_data(celebrities, movies, tv_shows)
    generate_sql_report()  # Generate SQL report
    export_to_excel()

if __name__ == "__main__":
    main()
