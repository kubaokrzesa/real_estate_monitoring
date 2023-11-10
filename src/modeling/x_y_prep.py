from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from src.modeling.modeling_funcs import CapTransformer


## X y
y = df['sq_m_price']
X = df.drop(columns=['sq_m_price'])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

# Define which columns you want to impute with the median
columns_to_impute_median = ['floor', 'max_floor', 'age_num']

# Initialize a dictionary to store your imputers
imputers = {}

# Create and fit a zero imputer for 'rent'
zero_imputer = SimpleImputer(strategy='constant', fill_value=0)
zero_imputer.fit(X_train[['rent']])
imputers['rent'] = zero_imputer

# Create, fit and store median imputers for the other columns
for column in columns_to_impute_median:
    median_imputer = SimpleImputer(strategy='median')
    median_imputer.fit(X_train[[column]])
    imputers[column] = median_imputer

# Apply the imputers to transform the training data
X_train['rent'] = imputers['rent'].transform(X_train[['rent']])
for column in columns_to_impute_median:
    X_train[column] = imputers[column].transform(X_train[[column]])

# Apply the imputers to transform the test data
X_test['rent'] = imputers['rent'].transform(X_test[['rent']])
for column in columns_to_impute_median:
    X_test[column] = imputers[column].transform(X_test[[column]])


# fix age column
age_cap_transformer = CapTransformer(column = 'age_num',min_value=-5, max_value=300)
X_train = age_cap_transformer.fit_transform(X_train)

X_test = age_cap_transformer.transform(X_test)