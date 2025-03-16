import kaggle
import pandas as pd
import sqlalchemy as sal
import pydoc

# Download Dataset (Uncomment this if you haven't downloaded it yet)
# !kaggle datasets download ankitbansal06/retail-orders -f orders.csv --path D:/Python/DataAnalyticsProject/Data > Run this on terminal to download


# Load CSV file with better error handling
#When reading a dataset, missing values (empty cells, "unknown", etc.) are automatically converted to NaN.
df = pd.read_csv("D:/Python/DataAnalyticsProject/Data/orders.csv", na_values = ["Not Available" , "unknown"], 
                 #encoding="ISO-8859-1", 
                # Use Python engine for flexibility
                 engine="python")  
# Display first 20 rows
#print(df.head(20))
df["Ship Mode"].unique()

df.columns =df.columns.str.lower()
#print(df.columns)

df.columns = df.columns.str.replace(" " , "_")
#print(df.columns)
#Display first 5 row
#print(df.head(5))


# Add a discount column 
df['discount']= df['list_price']*df['discount_percent']*.01
#print(df.head(5))

df['sale_price'] = df["list_price"] - df['discount']

df['profit'] = df['sale_price'] - df['cost_price']
#print(df.head(5))

#The errors='coerce' parameter is used in functions like pd.to_datetime() in pandas to specify how to 
# handle errors when converting a column (or series) of data to a datetime format.

df['order_date'] = pd.to_datetime(df['order_date'], format = "%y-%m-%d" , errors='coerce')
df.drop(columns=['list_price' , 'cost_price', 'discount_percent'], inplace= True)


engine = sal.create_engine('mssql://Legion/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
conn = engine.connect()

df.to_sql('df_orders' , con=conn  , index = False , if_exists='append')
