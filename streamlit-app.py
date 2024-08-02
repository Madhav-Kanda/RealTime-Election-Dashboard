import streamlit as st
import time
import psycopg2
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh
import plotly.express as px

# State name to code mapping
state_name_to_code = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD', 'Massachusetts': 'MA',
    'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO', 'Montana': 'MT',
    'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK',
    'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA', 'Washington': 'WA',
    'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'
}

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    
    # Fetch total number of voters
    cur.execute("SELECT COUNT(*) FROM voters")
    voters_count = cur.fetchone()[0]
    
    # Fetch total number of candidates
    cur.execute("SELECT COUNT(*) FROM candidates")
    candidates_count = cur.fetchone()[0]
    
    return voters_count, candidates_count

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.xticks(rotation=90)
    plt.title('Total Votes by Candidate')
    return plt

def plot_donut_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, wedgeprops=dict(width=0.3))
    ax.axis('equal')
    plt.title("Candidates' Votes Distribution")
    return fig

def plot_us_map(state_results):
    # Map state names to codes
    state_results['state_code'] = state_results['state'].map(state_name_to_code)
    fig = px.choropleth(state_results,
                        locations='state_code', 
                        locationmode="USA-states", 
                        color='leading_party',
                        scope="usa",
                        color_discrete_map={
                            "Demo Party": "blue",
                            "Republic Party": "red"
                        },
                        hover_name='state',
                        hover_data=['total_votes_demo', 'total_votes_republic'])
    fig.update_layout(geo_scope='usa')
    return fig

@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df

def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1)
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options=["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True)
    pagination = st.container()
    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        current_page = st.number_input("Page", min_value=1, max_value=total_pages, step=1)
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}**")
    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)

def update_data():
    last_refresh = st.empty()
    voters_count, candidates_count = fetch_voting_stats()
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)
    consumer = create_kafka_consumer("aggregated_votes_per_candidate")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)
    interim_results = results.groupby('candidate_id').agg({
    'candidate_name': 'first',
    'party_affiliation': 'first',
    'photo_url': 'first',
    'total_votes': 'sum'}).reset_index()
    leading_candidate = interim_results.loc[interim_results['total_votes'].idxmax()]
   
    location_consumer = create_kafka_consumer('aggregated_turnout_by_location')
    location_data = fetch_data_from_kafka(location_consumer)
    location_result = pd.DataFrame(location_data)
    location_result = location_result.reset_index(drop=True)
    location_result = location_result.groupby('state').agg({
    'total_votes_demo': 'sum',
    'total_votes_republic': 'sum'}).reset_index()

    # Determine leading party by state
    location_result['leading_party'] = location_result.apply(lambda row: 'Demo Party' if row['total_votes_demo'] > row['total_votes_republic'] else 'Republic Party', axis=1)
    
    demo_states = (location_result['total_votes_demo'] > location_result['total_votes_republic']).sum()
    republic_states = (location_result['total_votes_demo'] < location_result['total_votes_republic']).sum()

    demo_votes = interim_results[interim_results['party_affiliation'] == 'Demo Party']['total_votes'].values[0]
    republic_votes = interim_results[interim_results['party_affiliation'] == 'Republic Party']['total_votes'].values[0]

    us_map = plot_us_map(location_result)
    st.plotly_chart(us_map)


    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="States where Demo Party leads", value=demo_states, delta=float(demo_states - republic_states))
        st.metric(label="Votes won by Demo Party", value = demo_votes, delta=int(demo_votes-republic_votes))

    with col2:
        st.metric(label="States where Republic Party leads", value=republic_states, delta=float(republic_states - demo_states))
        st.metric(label="Votes won by Republic Party", value = republic_votes, delta=int(republic_votes - demo_votes))

    st.header("Leading Candidate (Max number of states won)")
    if(demo_votes>republic_votes): leading_candidate = interim_results[interim_results['party_affiliation'] == 'Demo Party'].iloc[0]
    else: leading_candidate = interim_results[interim_results['party_affiliation'] == 'Demo Party'].iloc[0]
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']}")
    st.markdown("""----""")

    st.header('Location of Voters')
    paginate_table(location_result)


def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()
    st_autorefresh(interval=5 * 1000, key="auto")

st.title("RealTime Presidential Election Results")
sidebar()
update_data()
