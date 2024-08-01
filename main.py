import psycopg2

def create_tables(conn, cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates(
        candidate_id VARCHAR(255) PRIMARY KEY,
        candidate_name VARCHAR(255),
        party_affiliation VARCHAR(255),
        biography TEXT,
        campaign_platform TEXT,
        photo_url TEXT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters(
            voter_id VARCHAR(255) primary key,
            voter_name VARCHAR(255),
            date_of_birth DATE,
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
            )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes(
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote int DEFAULT 1,
                primary key (voter_id, candidate_id)
            )  
        """)
    
    conn.commit()

if __name__ =='__main__':
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        create_tables(conn, cur)

    except Exception as e:
        print(e)