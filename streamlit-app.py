import streamlit as st
import time
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute("""
            SELECT count(*) voters_count from voters
    """)
    voters_count = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute(""" 
            SELECT count(*) candidates_count from candidates
    """)

    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x:json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data=[]

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)

    return data

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    voters_count, candidates_count = fetch_voting_stats()

    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # identify the leading candidates
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]

    # Display the leading candidate information
    st.markdown("""----""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader('Total Vote:{}'.format(leading_candidate['total_votes']))


st.title("Realtime Voting Dashboard")
topic_name = "aggregated_votes_per_candidates"


update_data()