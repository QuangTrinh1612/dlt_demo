import duckdb

# Create / connect to database
con = duckdb.connect('demo_db.duckdb')

tables = con.sql('SHOW ALL TABLES')
tables.show()

pokemons = con.sql('SELECT * FROM pokemon.pokemon LIMIT 100')
pokemons.show()