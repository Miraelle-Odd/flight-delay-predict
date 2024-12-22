import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.preprocessing import StandardScaler

class ModelPredictor:
    def __init__(self, df):
        self.df = df

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
            model.fit(train_X, train_y)
            predictions = model.predict(test_X)

            # Evaluate metrics
            rmse = np.sqrt(mean_squared_error(test_y, predictions))
            mae = mean_absolute_error(test_y, predictions)
            r2 = r2_score(test_y, predictions)

            print(f"{name} Results:")
            print(f"  - RMSE: {rmse:.4f}")
            print(f"  - MAE: {mae:.4f}")
            print(f"  - R² Score: {r2:.4f}")


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


if __name__ == "__main__":
    # Read Data from Cassandra
    keyspace = "testframe"
    table = "flightdelay"
    df = read_from_cassandra(keyspace, table)

    # Specify columns to keep
    columns_to_keep = ["dep_delay", "dep_delay_new", "dep_del15", "dep_delay_group", "arr_delay_new", "arr_del15", "arr_delay_group", "arr_delay"]  # Update with relevant columns
    predictor = ModelPredictor(df)
    df_cleaned = predictor.keep_relevant_columns(columns_to_keep)

    # Prepare Data for Training
    label_column = "arr_delay"  # Target variable
    train_X, test_X, train_y, test_y = predictor.prepare_data(label_column)

    # Train and Evaluate Models
    predictor.train_and_evaluate(train_X, test_X, train_y, test_y)
