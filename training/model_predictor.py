import pandas as pd
import numpy as np
import time
import pickle  # For serialization
from datetime import datetime  # For unique timestamps
from cassandra.cluster import Cluster
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

class ModelPredictor:
    def __init__(self, df, cassandra_ip="127.0.0.1", cassandra_port=9042, keyspace="testframe"):
        self.df = df
        self.cassandra_ip = cassandra_ip
        self.cassandra_port = cassandra_port
        self.keyspace = keyspace

    def keep_relevant_columns(self, columns_to_keep):
        """Keep only the necessary columns."""
        self.df = self.df[columns_to_keep]
        return self.df

    def prepare_data(self, label_col):
        """Prepare features and target, and split data."""
        X = self.df.drop(columns=[label_col])
        y = self.df[label_col]
        
        # Scale the features for better performance
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        train_X, test_X, train_y, test_y = train_test_split(X_scaled, y, test_size=0.2, random_state=42)
        return train_X, test_X, train_y, test_y

    def train_and_evaluate(self, train_X, test_X, train_y, test_y):
        """Train and evaluate models."""
        models = {
            "Linear Regression": LinearRegression(),
            "Random Forest Regressor": RandomForestRegressor(n_estimators=10, random_state=42),
            "Decision Tree Regressor": DecisionTreeRegressor(max_depth=5, random_state=42)
        }

        for name, model in models.items():
            print(f"\nTraining {name}...")

            # Measure training time
            start_time = time.time()
            model.fit(train_X, train_y)
            end_time = time.time()
            training_time = end_time - start_time

            predictions = model.predict(test_X)

            # Evaluate metrics
            rmse = np.sqrt(mean_squared_error(test_y, predictions))
            mae = mean_absolute_error(test_y, predictions)
            r2 = r2_score(test_y, predictions)

            print(f"{name} Results:")
            print(f"  - Training Time: {training_time:.2f} seconds")
            print(f"  - RMSE: {rmse:.4f}")
            print(f"  - MAE: {mae:.4f}")
            print(f"  - R² Score: {r2:.4f}")

            # Create a unique model name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_model_name = f"{name}_{timestamp}"

            # Save the trained model to the database
            self.save_model_to_db(unique_model_name, model, rmse, mae, r2)

    def save_model_to_db(self, model_name, model, rmse, mae, r2):
        """Serialize the model and save it to Cassandra."""
        # Serialize the model using pickle
        model_binary = pickle.dumps(model)

        # Connect to Cassandra
        cluster = Cluster([self.cassandra_ip], port=self.cassandra_port)
        session = cluster.connect()
        session.set_keyspace(self.keyspace)

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS models (
            model_name TEXT PRIMARY KEY,
            model_data BLOB,
            rmse FLOAT,
            mae FLOAT,
            r2 FLOAT,
            training_time TIMESTAMP
        );
        """
        session.execute(create_table_query)

        # Insert model into the database
        insert_query = """
        INSERT INTO models (model_name, model_data, rmse, mae, r2, training_time)
        VALUES (%s, %s, %s, %s, %s, toTimestamp(now()));
        """
        session.execute(insert_query, (model_name, model_binary, rmse, mae, r2))

        print(f"Model '{model_name}' saved to database.")

        # Close the connection
        cluster.shutdown()

def read_from_cassandra(keyspace, table, cassandra_ip="127.0.0.1", cassandra_port=9042):
    """Read data from a Cassandra table into a Pandas DataFrame."""
    cluster = Cluster([cassandra_ip], port=cassandra_port)
    session = cluster.connect()
    session.set_keyspace(keyspace)

    query = f"SELECT * FROM {table};"
    rows = session.execute(query)
    df = pd.DataFrame(rows.current_rows)

    cluster.shutdown()
    return df

def read_from_csv(file_path="../Final_Data/kg-flightdelay-dataset/stream_data.csv", n_rows=None):
    """Read data from a CSV file with an optional limit on the number of rows."""
    df = pd.read_csv(file_path, nrows=n_rows)
    return df


def process_dataframe(spark_df):
    columns_to_keep = ["dep_delay", "dep_delay_new", "dep_del15", "dep_delay_group", "arr_delay_new", "arr_del15", "arr_delay_group", "arr_delay"]  # Update with relevant columns
    label_column = "arr_delay"  # Target variable
    
    """Convert Spark DataFrame to Pandas and process it."""
    # Convert Spark DataFrame to Pandas
    pandas_df = spark_df.toPandas()

    # Initialize ModelPredictor with Pandas DataFrame
    predictor = ModelPredictor(pandas_df)

    # Keep relevant columns
    df_cleaned = predictor.keep_relevant_columns(columns_to_keep)

    # Prepare data
    train_X, test_X, train_y, test_y = predictor.prepare_data(label_column)

    # Train and evaluate models
    predictor.train_and_evaluate(train_X, test_X, train_y, test_y)

if __name__ == "__main__":
    use_cassandra = False  
    if use_cassandra:
        # Read Data from Cassandra
        keyspace = "testframe"
        table = "flightdelay"
        df = read_from_cassandra(keyspace, table)
        columns_to_keep = ["dep_delay", "dep_delay_new", "dep_del15", "dep_delay_group", "arr_delay_new", "arr_del15", "arr_delay_group", "arr_delay"]  # Update with relevant columns
        label_column = "arr_delay"  # Target variable


    else:
        df = read_from_csv(n_rows=300000)
        columns_to_keep = ["DEP_DELAY", "DEP_DELAY_NEW", "DEP_DEL15", "DEP_DELAY_GROUP", "ARR_DELAY_NEW", "ARR_DEL15", "ARR_DELAY_GROUP", "ARR_DELAY"]  # Update with relevant columns
        label_column = "ARR_DELAY"  # Target variable


    # Specify columns to keep
    predictor = ModelPredictor(df)
    df_cleaned = predictor.keep_relevant_columns(columns_to_keep)

    # Prepare Data for Training
    train_X, test_X, train_y, test_y = predictor.prepare_data(label_column)

    # Train and Evaluate Models
    predictor.train_and_evaluate(train_X, test_X, train_y, test_y)
