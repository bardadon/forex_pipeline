from sklearn.ensemble import RandomForestRegressor
import pandas as pd 
import numpy as np
import datetime
from functools import lru_cache

def import_rates():

    try:
        # Import rates
        print('Importing Daily Rates...')
        daily_rates = pd.read_csv('Foreign_Exchange_Rates.csv', index_col='Unnamed: 0')
        print('Done')
        return daily_rates
    except Exception as e:
        print('Issue with Importing Daily Rates')
        print(e)
        

'''
Clean DataFrame
'''

def clean_dataframe(daily_rates):
    '''
    Every column has 198 missing values called: 'ND'.
    Replace ND with the average rate per currency.
    Args:
        - daily_rates(Pandas DataFrame)
    Returns:
        - daily_rates(Pandas DataFrame)
    '''
 
    try:
        print('\nRemoving Missing Values...')
        daily_rates.replace({'ND':np.nan}, inplace = True)
        daily_rates.dropna(inplace=True)
        daily_rates.iloc[:,1:] = daily_rates.iloc[:,1:].astype('float')            
        daily_rates = daily_rates.reset_index()
        daily_rates.drop(axis = 1, columns=['index'], inplace=True)
        print('Done')
        return daily_rates
    except Exception as e:
        print(e)

    

def generate_dates():
    '''
    Generate one second timestamps across a full year
    '''
    try:

        print('\nGenerating Dates...')
        dates = []

        for j in range(1,13): # month
            for i in range(1,28): # days
                for z in range(0,24): # hours
                    for h in range(1,60): # minutes
                        for g in range(1,60): # seconds
                            dates.append(datetime.datetime(2022,j,i,z,h,g))
        print('Done')
    except Exception as e:
        print(e)

    return dates


'''
Generate Data
'''
def generate_data(daily_rates, dates):

    '''
    Generating currency rates for each second
    Args:
        - daily_rates(Pandas DataFrame)
    Returns:
        - daily_rates(Pandas DataFrame) 
    '''

    try:
        print('\nGenerating The Following Currencies:')
        # Create a Random Forest model
        model = RandomForestRegressor(n_estimators=1, n_jobs = -1, max_depth=500, random_state=1, max_leaf_nodes=40000) 

        # Set values
        x = np.arange(len(daily_rates.iloc[:,0])).reshape(-1,1)
        currency_list = daily_rates.columns
        rates_df = pd.DataFrame(dates, columns=['date'])

        # Iterate through the countries
        for country in range(1,len(currency_list)-1):

            # For each country grab the rates and the name
            currency_rates = daily_rates.iloc[:,country]
            currency_name = str(currency_list[country])
            print(currency_name)

            # Create a Random Forest model and predict the rates for the entire year
            model.fit(x, currency_rates)
            rates_prediction = model.predict(np.arange(len(dates)).reshape(-1,1))

            # Add random noise to prediction(because the dataset is really small)
            noise = np.random.normal(0, currency_rates.std()/10, len(dates)) 
            rates_prediction = rates_prediction + noise

            # Remove outliers
            q_01 = pd.Series(rates_prediction).quantile(0.01)
            q_99 = pd.Series(rates_prediction).quantile(0.99)
            rates_prediction[rates_prediction<q_01] = q_01
            rates_prediction[rates_prediction>q_99] = q_99


            # Add predicted rates to the DatFrame
            rates_df[currency_name] = rates_prediction
            print('Done')
    except Exception as e:
        print('An Error Occurred.')
        print(e)
    finally:
        # Export to CSV
        print('Export to CSV...')
        rates_df.set_index('date', inplace = True)
        rates_df.to_csv('rates.csv')
        print('\nDone')
        return rates_df


if __name__ == "__main__":

    daily_rates = import_rates()
    
    clean_rates = clean_dataframe(daily_rates)

    dates = generate_dates()

    rates_df = generate_data(daily_rates, dates)