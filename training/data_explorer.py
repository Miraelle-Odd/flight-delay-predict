import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from cassandra.cluster import Cluster

class DataExplorer:
    def __init__(self, df):
        self.df = df

    def show_schema(self):
        print("Schema of the DataFrame:")
        print(self.df.dtypes)

    def show_sample(self, n=5):
        print(f"First {n} Rows:")
        print(self.df.head(n))

    def count_missing_values(self):
        print("Missing Values Count:")
        print(self.df.isnull().sum())

    def show_summary(self):
        print("Data Summary:")
        print(self.df.describe(include='all'))

    def visualize_distribution(self, column_name):
        print(f"Visualizing distribution of column: {column_name}")
        plt.figure(figsize=(10, 6))
        sns.histplot(self.df[column_name].dropna(), kde=True)
        plt.title(f"Distribution of {column_name}")
        plt.xlabel(column_name)
        plt.ylabel("Frequency")
        plt.show()

    def visualize_correlation(self, columns):
        print(f"Visualizing correlation between columns: {columns}")
        correlation_matrix = self.df[columns].corr()
        plt.figure(figsize=(10, 8))
        sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap="coolwarm", cbar=True)
        plt.title("Correlation Matrix")
        plt.show()

    def show_heatmap(self, target_col="ARR_DELAY"):
        numeric_cols = self.df.select_dtypes(include=['number']).columns.tolist()
        if target_col not in numeric_cols:
            print(f"Target column '{target_col}' is not numeric.")
            return

        correlation_data = self.df[numeric_cols].corr()[target_col].drop(target_col)
        correlation_df = correlation_data.reset_index()
        correlation_df.columns = ["Feature", "Correlation with " + target_col]

        plt.figure(figsize=(12, 8))  # Increase figure size for readability
        sns.heatmap(
            correlation_df.set_index("Feature").T, 
            annot=True, 
            cmap="coolwarm", 
            fmt=".2f", 
            cbar=True
        )
        plt.title(f"Correlation Heatmap: Features vs {target_col}")
        plt.xticks(rotation=45, ha='right')  # Rotate labels for readability
        plt.tight_layout()  # Adjust layout to fit everything
        plt.show()

    def visualize_categorical_count(self, column_name):
        print(f"Visualizing categorical counts of column: {column_name}")
        plt.figure(figsize=(10, 6))
        sns.countplot(data=self.df, x=column_name, order=self.df[column_name].value_counts().index)
        plt.title(f"Count of Categories in {column_name}")
        plt.xlabel(column_name)
        plt.ylabel("Count")
        plt.xticks(rotation=45)
        plt.show()

if __name__ == "__main__":
    # Connect to Cassandra
    cluster = Cluster(["127.0.0.1"], port=9042)  # Adjust IP and port as needed
    session = cluster.connect()

    # Select the keyspace and table
    keyspace = "testframe"
    table = "flightdelay"
    session.set_keyspace(keyspace)

    # Execute the query to fetch data
    query = f"SELECT * FROM {table}"
    rows = session.execute(query)
    print(rows)
    # Convert to Pandas DataFrame
    df = pd.DataFrame(rows)

    # Initialize DataExplorer with the DataFrame
    explorer = DataExplorer(df)

    # Perform operations
    explorer.show_schema()
    explorer.show_sample()
    explorer.count_missing_values()
    explorer.show_summary()
    explorer.show_heatmap(target_col="arr_delay")
    # explorer.visualize_distribution("dep_delay")
    # explorer.visualize_distribution("dep_delay_new")
    # explorer.visualize_distribution("dep_del15")
    # explorer.visualize_distribution("dep_delay_group")
    # explorer.visualize_distribution("arr_delay_new")
    # explorer.visualize_distribution("arr_del15")
    # explorer.visualize_distribution("dep_delay_group")
    # explorer.visualize_distribution("arr_delay")

    cluster.shutdown()
