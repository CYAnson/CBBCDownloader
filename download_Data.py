import requests
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt


# Input url and downlaod the .zip file to the folder
# From link: https://www.hkex.com.hk/eng/cbbc/download/dnCSV.asp
# Link for Current Month: https://www.hkex.com.hk/eng/cbbc/download/CBBC11.zip

def download_zip_from_hkex(url, save_name, chunk_size=128):
    r = requests.get(url, stream=True)
    # Write the file as .zip and save as {save_name}
    with open(save_name, 'wb') as output:
        for chunk in r.iter_content(chunk_size=chunk_size):
            output.write(chunk)

def bulk_download_zip_fram_hkex(start_month, end_month, combine):
    datelist = pd.date_range(start=start_month, end=end_month, freq='M')
    datelist = [d.strftime("%Y%m") for d in datelist]

    for month in datelist:
        print(f"Downloading {month} data")
        download_zip_from_hkex(f'https://www.hkex.com.hk/eng/cbbc/download/CBBC{month[-2:]}.zip', f'CBBC_{month}.zip',
                               chunk_size=128)
    if combine == "Y":
        cbbc_full = pd.DataFrame()
        for month in datelist:
            print('Appending ', month)
            raw = pd.read_csv(f'CBBC_{month}.zip', compression='zip', header=0, sep='\t', encoding='utf-16')
            raw = raw[:-3]
            cbbc_full = cbbc_full.append(raw)
        cbbc_full.to_csv("cbbc_full" + "_" + str(start_month) + "_" + str(end_month)+'.csv')
        print(cbbc_full)

def combine_csv_method():
    cbbc_full = pd.DataFrame()
    monthlist = ['01', '02', '03', '04', '05', '06', '07']
    #monthlist = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    #monthlist = ['09', '10', '11', '12']
    for month in monthlist:
        print('Appending ', month)
        #raw = pd.read_csv("C:/Users/anson/PycharmProjects/CBBCAlgo/Data/CBBC2020" + str(month) +'.csv', header = 0, encoding='utf-8') # For 2018 / 2019
        raw = pd.read_csv("C:/Users/anson/PycharmProjects/CBBCAlgo/Data/CBBC2021" + str(month) + '.csv', header=0, sep='\t', encoding='utf-16')     # For 2020 / 2021
        raw = raw[:-3]
        cbbc_full = cbbc_full.append(raw)
    cbbc_full.to_csv("cbbc_full" + "_" + "2021" + '.csv')



def read_update_file(file_name):
    #raw = pd.read_csv(str(zip_name), compression="zip", sep="\t", encoding="utf-16")   # for zip_file type
    raw = pd.read_csv(file_name)
    raw['Bull/Bear'] = raw['Bull/Bear'].str.strip()
    raw = raw[raw['Last Trading Date'] != raw['Trade Date']]      # Filter out CBBC that expired
    raw['Trade Date'] = raw['Trade Date'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')   # Turn date to datetime format
    raw['future'] = (raw['No. of CBBC still out in market *']) / raw['Ent. Ratio^'] / 100 * 2   # Calucate the relative number of Futures
    raw = raw.groupby(['Underlying', 'Trade Date', 'Bull/Bear'])['future'].sum()['HSI'].to_frame() # Pivot table of the figures
    df = pd.pivot_table(raw, values=['future'], index=['Trade Date'], columns=['Bull/Bear'])['future']
    print(df)
    df.to_csv('cbbc_2021' + '_ReadyUse' + '.csv') # Update Data remain the Bull & Bear Data
    plt.plot(df['Bull'], label='Bull')
    plt.plot(df['Bear'], label='Bear')
    plt.legend()
    plt.show()
    return df


if __name__ == "__main__":
    #download_zip_from_hkex('https://www.hkex.com.hk/eng/cbbc/download/CBBC10.zip', 'CBBC_202010.zip', chunk_size=128)
    #bulk_download_zip_fram_hkex("2021-01-01", "2021-08-01", "Y")
    read_update_file('cbbc_full_2021.csv')
    #combine_csv_method()
