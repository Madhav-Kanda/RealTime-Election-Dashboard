# RealTime Presidential Election Results

This project is a Streamlit-based web application that provides real-time visualization of dummy presidential election results in the United States. It uses Kafka for real-time data streaming, PostgreSQL for database management, Apache Spark for data processing, and Docker for containerization. The application displays various metrics including total voters, votes by candidate, and leading parties by state, along with interactive charts and maps.

## Features

- Real-time updates of election results using Kafka.
- Visualization of total voters.
- Choropleth map showing leading parties by state.
- Paginated table displaying location-based voter turnout.
- Data processing using Apache Spark.
- Containerized deployment using Docker.

## Technologies Used

- [Streamlit](https://streamlit.io/) - Web application framework.
- [Kafka](https://kafka.apache.org/) - Real-time data streaming platform.
- [PostgreSQL](https://www.postgresql.org/) - Relational database management system.
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine for large-scale data processing.
- [Docker](https://www.docker.com/) - Containerization platform.
- [Plotly](https://plotly.com/python/) - Interactive graphing library.
- [Pandas](https://pandas.pydata.org/) - Data manipulation and analysis library.
- [Matplotlib](https://matplotlib.org/) - Plotting library for Python.
- [simplejson](https://simplejson.readthedocs.io/en/latest/) - JSON encoder and decoder.

## Installation

### Prerequisites

- Python 3.7 or later
- Docker and Docker Compose
- Kafka server running locally or accessible
- PostgreSQL database running locally or accessible
- Virtual environment tool (optional but recommended)

### Steps

1. **Clone the repository:**

   ```sh
   git clone https://github.com/your-username/election-results.git
   cd election-results

2. Create a virtual environment and activate it:

   ```sh
   python -m venv .venv
   # On Windows
   .venv\Scripts\activate
   # On macOS and Linux
   source .venv/bin/activate
   
3. Install the required dependencies:
   ```sh
   pip install -r requirements.txt

4. Run Docker containers:
    Ensure Docker is installed and running.
    Build and run the Docker containers:
     ```sh
     docker-compose up --build

5. Create the databases in PostgreSQL and Insert the entries for candidates and voters using main.py:
   ```sh
   python main.py

6. Insert the entries for votes:
   ```sh
   python voting.py

7. For processing data in real time using spark:
   ```sh
   python spark-streaming.py

8. Run the Streamlit application:
   ```sh
   streamlit run streamlit-app.py

## Configuration
- Configure the following paths in spark-straming.py file:
   -  postgresql-42.7.3.jar
   -  Checkpoints folders

## Usage
- The application will start and be accessible in your web browser.
- The main page displays total voters, various charts, and a map showing election results.
- The page auto-refreshes every 5 seconds to fetch the latest data.

## Project Structure
```plaintext
.
├── app.py                # Main application script that sets up Streamlit interface and displays data
├── docker-compose.yml    # Docker Compose configuration file to set up and run the containers
├── Dockerfile            # Dockerfile for building the Docker image for the application
├── spark_streaming.py    # Spark Streaming script for processing data in real-time
├── requirements.txt      # Python dependencies required for the project
├── README.md             # Project documentation
├── checkpoints/          # Directory for Spark Streaming checkpoints
    ├── checkpoint1/
    ├── checkpoint2/
    ├── checkpoint3/
```
## Screenshots
<img width="1470" alt="image" src="https://github.com/user-attachments/assets/a515399c-9256-4e49-8be3-4e33ba070dbd">
<img width="1470" alt="image" src="https://github.com/user-attachments/assets/24ac035e-d2de-4b3f-8d99-eac000e488e4">
<img width="1470" alt="image" src="https://github.com/user-attachments/assets/d6361f77-05d3-4601-acc9-ce37c344b1cc">

