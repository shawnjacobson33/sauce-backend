import pandas as pd

# Sample data
data = {
    'Category': ['A', 'A', 'B', 'B', 'C', 'C', 'C'],
    'Values': [10, 20, 15, 30, 45, 50, 40]
}
df = pd.DataFrame(data)

grouped = df.groupby('Category').agg({
    'Values': ['sum', 'count']
}).reset_index()

asd = 123